(ns elastic-migrate.core
  (:gen-class)
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.rest.bulk :as bulk]
            [clj-time.core :as tc]
            [clj-time.format :as tf]))

(def counter (atom 0))
(def write-counter (atom 0))
(def started (tc/now))
(def simple-formatter (tf/formatter "HH:mm:ss"))

(def conf {:es-address "http://127.0.0.1:9200"
           :es-index "ferguson"
           :mapping "tweet"})

(esr/connect! (:es-address conf))

(defn get-tweet [id]
  "get Tweet for specified ID"
  (esd/get (:es-index conf) "tweet" id))

(defn process-tweet [doc]
  (let [tweet (:_source doc)
        rt-status (:retweeted_status tweet)]
    (when (and rt-status (not (get-tweet (:id_str rt-status))))
      (esd/put (:es-index conf) (:mapping conf) (:id_str rt-status) rt-status)
      (swap! write-counter inc))
    (swap! counter inc)
    (if (= (mod @counter 1000) 0)
      (println (tf/unparse simple-formatter (tc/now)) @counter "items processed -" @write-counter "written"))))

(defn lazy-find []
  (esd/scroll-seq
   (esd/search
    (:es-index conf)
    (:mapping conf)
    :query (q/match-all)
    :search_type "query_then_fetch"
    :sort {:id "desc"}
    :scroll "1h"
    :size 1000)))

(defn -main
  [& args]
  (println (tf/unparse simple-formatter (tc/now)) "Processing started")
  (dorun (map process-tweet (lazy-find)))
  (printf "Processed %,d tweets in %,d seconds\n" @counter (tc/in-seconds (tc/interval started (tc/now)))))

