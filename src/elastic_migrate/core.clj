(ns elastic-migrate.core
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query :as q]
                       [clojurewerkz.elastisch.rest.index         :as idx]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.rest.bulk :as bulk]
            [clojure.pprint :as pp]
            [clj-time.core :as tc]
            [clj-time.format :as tf]))

(def counter (atom 0))
(def insert-ops (atom []))
(def started (tc/now))
(def simple-formatter (tf/formatter "HH:mm:ss"))

(esr/connect! "http://127.0.0.1:9200")

(defn batch-insert [ops]
  (let [insert-operations (bulk/bulk-index ops)]
    (bulk/bulk insert-operations :refresh true)                                         )
  [])

(defn process-tweet [doc]
  (let [tweet (:_source doc)
        for-index (assoc tweet :_index "birdwatch_v2" :_type "tweets" :_id (:id_str tweet))]
    (swap! insert-ops conj for-index)
    (swap! counter inc)
    (if (= (mod @counter 1000) 0)
      (do
        (swap! insert-ops batch-insert)
        (println (tf/unparse simple-formatter (tc/now)) @counter "items processed")))))

(defn lazy-find []
  (esd/scroll-seq
   (esd/search
    "birdwatch_tech"
    "tweets"
    :query (q/match-all)
    :search_type "query_then_fetch"
    :scroll "10h"
    :size 1000)))

(defn -main
  [& args]
  (println (tf/unparse simple-formatter (tc/now)) "Processing started")
  (dorun (map process-tweet (lazy-find)))
  (printf "Processed %,d tweets in %,d seconds\n" @counter (tc/in-seconds (tc/interval started (tc/now)))))

