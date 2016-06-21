(defproject schedule-clj "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0-alpha7"]
                 [aysylu/loom "0.6.0"]]
  :plugins [[cider/cider-nrepl "0.13.0-snapshot"]]
  :main ^:skip-aot schedule-clj.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
