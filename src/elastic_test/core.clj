(ns elastic-test.core
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojure.pprint :as pp]
            [clj-time.core :as tc]
            [clj-time.format :as tf]))

(def counter (atom 0))
(def started (tc/now))

(def twitter-formatter (tf/formatter "E MMM dd HH:mm:ss Z yyyy"))
(def el-formatter (tf/formatter "yyyyMMdd'T'HHmmssZ"))
(def simple-formatter (tf/formatter "HH:mm:ss"))

(defn date-format [s]
  (if (nil? s)
    nil
    (tf/unparse el-formatter (tf/parse twitter-formatter s))))

(esr/connect! "http://127.0.0.1:9200")
(defn change-df [tweet]
  (let [t1 (update-in tweet [:created_at] date-format)
        t2 (update-in t1 [:user :created_at] date-format)
        t3 (update-in t2 [:retweeted_status :created_at] date-format)
        t4 (update-in t3 [:retweeted_status :user :created_at] date-format)]
    t4))

(defn process-tweet [doc]
  (let [tweet (:_source doc)
        text (:text tweet)]
    (esd/create "birdwatch_v2" "tweets" (change-df tweet))
    (swap! counter inc)
    (if (= (mod @counter 10000) 0) (println (tf/unparse simple-formatter (tc/now)) @counter "items processed"))
    text
    ))

(defn lazy-find []
  (esd/scroll-seq
   (esd/search
    "birdwatch_tech"
    "tweets"
    :query (q/match-all)
;    :query (q/query-string :query "java"
;                           :allow_leading_wildcard false
;                           :default_operator "AND")
    :search_type "query_then_fetch"
    :scroll "10h"
    :size 1000)))

(defn -main
  [& args]
  (println (tf/unparse simple-formatter (tc/now)) "Processing started")
  (dorun (map process-tweet (lazy-find)))
  (printf "Processed %,d tweets in %,d seconds\n" @counter (tc/in-seconds (tc/interval started (tc/now)))))

