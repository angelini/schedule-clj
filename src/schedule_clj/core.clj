(ns schedule-clj.core
  (:require [clojure.spec :as s]
            [loom.alg :refer [dag?]]
            [loom.graph :refer [digraph add-edges out-edges edges]]
            [loom.io :refer [dot-str]]))

(s/def ::id-type   int?)
(s/def ::time-type int?)

(s/def ::id     ::id-type)
(s/def ::store  #{:hdfs})
(s/def ::path   string?)
(s/def ::start  ::time-type)
(s/def ::stop   ::time-type)
(s/def ::status #{:new :in-progress :done})

(s/def ::ds-id  ::id-type)
(s/def ::in-id  ::id-type)
(s/def ::out-id ::id-type)

(s/def ::dataset    (s/keys :req-un [::id ::store ::path ::start ::stop]))
(s/def ::datasets   (s/* ::dataset))
(s/def ::dataset-id ::id-type)

(s/def ::job    (s/keys :req-un [::id ::ds-id ::path]))
(s/def ::jobs   (s/* ::job))
(s/def ::job-db ::id-type)

(s/def ::source    (s/keys :req-un [::id ::in-id ::out-id]))
(s/def ::sources   (s/* ::source))
(s/def ::source-id ::id-type)

(s/def ::change    (s/keys :req-un [::id ::ds-id ::start ::stop ::status]))
(s/def ::changes   (s/* ::change))
(s/def ::change-id ::id-type)

(s/def ::db
  (s/keys :req-un [::dataset-id
                   ::datasets
                   ::job-id
                   ::jobs
                   ::source-id
                   ::sources
                   ::change-id
                   ::changes
                   ]))

(def initial-db-state
  {:dataset-id 5
   :datasets [{:id 1 :store :hdfs :path "/id/1" :start 0 :stop 10}
              {:id 2 :store :hdfs :path "/id/2" :start 0 :stop 10}
              {:id 3 :store :hdfs :path "/id/3" :start 0 :stop 22}
              {:id 4 :store :hdfs :path "/id/4" :start 0 :stop 8}
              {:id 5 :store :hdfs :path "/id/5" :start 0 :stop 14}
              ]
   :job-id 4
   :jobs [{:id 1 :ds-id 2 :path "/build-2.sh"}
          {:id 2 :ds-id 4 :path "/build-4.sh"}
          ]
   :source-id 3
   :sources [{:id 1 :in-id 1 :out-id 2}
             {:id 2 :in-id 2 :out-id 4}
             {:id 3 :in-id 3 :out-id 4}
             ]
   :change-id 1
   :changes [{:id 1 :ds-id 1 :start 10 :stop 12 :status :new}
             ]
   })

(def db (atom initial-db-state))

(defn reset-db! []
  (reset! db initial-db-state))

(defn validate-db []
  (s/explain ::db @db))

(defn by-key [coll key val]
  (filter #(= (get % key) val)
          coll))

(s/fdef dataset-by-id
        :args (s/cat :ds-id ::id-type)
        :ret ::dataset)
(defn dataset-by-id [ds-id]
  (first (by-key (:datasets @db) :id ds-id)))

(s/fdef change-by-id
        :args (s/cat :change-id ::id-type)
        :ret ::change)
(defn change-by-id [change-id]
  (first (by-key (:changes @db) :id change-id)))

(s/fdef changes-by-status
        :args (s/cat :status ::status)
        :ret ::changes)
(defn changes-by-status [status]
  (by-key (:changes @db) :status status))

(s/fdef dataset->sources
        :args (s/cat :ds-id ::id-type)
        :ret ::sources)
(defn dataset->sources [ds-id]
  (by-key (:sources @db) :out-id ds-id))

(s/fdef dataset->job
        :args (s/cat :ds-id ::id-type)
        :ret ::job)
(defn dataset->job [ds-id]
  (first (by-key (:jobs @db) :ds-id ds-id)))

(defn- add-dataset-to-graph [graph dataset]
  (let [edges (map (fn [source] [(:in-id source) (:out-id source)])
                   (dataset->sources (:id dataset)))]
    (if (not (empty? edges))
      (apply add-edges graph edges)
      graph)))

(s/fdef build-graph
        :ret dag?)
(defn build-graph []
  (reduce add-dataset-to-graph
          (digraph)
          (:datasets @db)))

(s/fdef new-job-from-candidate
        :args (s/cat :ds-id ::id-type :stop ::time-type)
        :ret ::job)
(defn new-job-from-candidate [ds-id stop]
  (let [dataset (dataset-by-id ds-id)]
    (if (> stop (:stop dataset))
      (dataset->job ds-id))))

(s/fdef new-jobs-from-change
        :args (s/cat :change ::change)
        :ret ::jobs)
(defn new-jobs-from-change [change]
  (let [ds-id (:ds-id change)
        graph (build-graph)
        candidates (map (fn [e] [(second e) (:stop change)])
                        (out-edges graph ds-id))]
    (->> candidates
         (map #(apply new-job-from-candidate %))
         (remove nil?))))

(s/fdef min-stop
        :args (s/cat :datasets (s/spec (s/+ ::dataset)))
        :ret int?)
(defn min-stop [datasets]
  (->> datasets
       (map :stop)
       (reduce min)))

(s/fdef run-job
        :args (s/cat :job ::job)
        :ret ::change)
(defn run-job [job]
  (let [ds-id (:ds-id job)
        dataset (dataset-by-id ds-id)
        sources (->> (dataset->sources ds-id)
                     (map :in-id)
                     (mapv dataset-by-id))]
    {:id (:change-id (swap! db update-in [:change-id] inc))
     :ds-id ds-id
     :start (:stop dataset)
     :stop (min-stop sources)
     :status :new}))

(defn- update-change-status [old new-ids status]
  (mapv (fn [change]
          (if (some #{(:id change)} new-ids)
            (assoc change :status status)
            change))
        old))

(defn- start-changes [db new-change-ids]
  (update-in db [:changes] update-change-status new-change-ids :in-progress))

;; FIXME: not thread safe
(s/fdef start-new-changes
        :ret ::changes)
(defn start-new-changes []
  (let [new-change-ids (map :id (changes-by-status :new))]
    (swap! db start-changes new-change-ids)
    new-change-ids))

(s/fdef add-and-finish-changes
        :args (s/cat :change-id ::id-type :changes (s/spec ::changes))
        :ret ::db)
(defn add-and-finish-changes [change-id changes]
  (swap! db
         (fn [db change-id changes]
           (-> db
               (update-in [:changes] update-change-status [change-id] :done)
               (update-in [:changes] #(apply conj % changes))))
         change-id
         changes))

(defn- update-dataset-stop [datasets id stop]
  (mapv (fn [dataset]
          (if (= (:id dataset) id)
            (assoc dataset :stop stop)
            dataset))
        datasets))

(defn- symbol-val [s]
  (subs (str s) 1))

(defn print-db []
  (let [d @db]
    (println "> datasets")
    (doseq [ds (:datasets d)]
      (println (format "%d - %s:/%s\t(%d %d]"
                       (:id ds) (symbol-val (:store ds)) (:path ds)
                       (:start ds) (:stop ds))))
    (println "\n> changes")
    (doseq [ch (:changes d)]
      (println (format "%d - %s for %d\t(%d %d]"
                       (:id ch) (symbol-val (:status ch)) (:ds-id ch)
                       (:start ch) (:stop ch))))
    (println "\n> graph")
    (doseq [edge (edges (build-graph))]
      (println (format "%s -> %s"
                       (first edge) (second edge))))))

(defn step []
  (let [new-change-ids (start-new-changes)]
    (doseq [change-id new-change-ids]
      (let [change (change-by-id change-id)]
        (println "> processing " change)
        (swap! db update-in [:datasets] update-dataset-stop (:ds-id change) (:stop change))
        (->> change
             new-jobs-from-change
             (map run-job)
             (add-and-finish-changes change-id))
        (print-db)))))

(s/instrument-all)
