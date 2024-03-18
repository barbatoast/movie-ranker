(ns ranker.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io  :as io]
            [clojure.string   :as string]
            [clojure.walk     :as walk])
  (:gen-class))

(defn parse-csv-file [csv-file-path]
  (let [csv-data (with-open [reader (io/reader csv-file-path)]
                   (doall (csv/read-csv reader)))
        headers  (first csv-data)
        body     (rest csv-data)]
    (->> body
         (map (partial zipmap headers)))))

(defn parse-films [films]
  (map (fn [x] {:title (x :Film)
                :score (Integer/parseInt (x (keyword "Audience score %")))})
       films))

(defn get-ranks [films]
  (map-indexed (fn [idx film] {:title (film :title)
                               :rank (+ idx 1)
                               :score (film :score)})
               films))

(defn common-prefix [sep paths]
  (let [parts-per-path (map #(string/split % (re-pattern sep)) paths)
        parts-per-position (apply map vector parts-per-path)]
    (string/join sep
                 (for [parts parts-per-position :while (apply = parts)]
                   (first parts)))))

(defn get-prefixes [ranks]
  (map (fn [x] {:title (x :title)
                :rank (x :rank)
                :score (x :score)
                :prefix (let [r1 (map (fn [y] (common-prefix "" [(x :title)
                                                                 (y :title)]))
                                      ranks)
                              r2 (filter (fn [x] (> (count x) 4)) r1)]
                          (reduce (fn [x y] (if (< (count x) (count y)) x y))
                                  r2))})
       ranks))

(defn remove-duplicates [prefixes]
  (let [r1 (group-by :prefix prefixes)
        r2 (map (fn [[_k v]]
                  (reduce (fn [x y] (if (< (x :rank) (y :rank)) x y)) v))
                r1)]
    (sort-by :rank r2)))

(defn rerank [dedup]
  (reductions (fn [prev cur]
                {:title (cur :title)
                 :score (cur :score)
                 :rank (+ (prev :rank) 1)}) dedup))

(defn rank-movies [csv-file-path]
  (->> csv-file-path
       (parse-csv-file)
       (map walk/keywordize-keys)
       (parse-films)
       (sort-by :score)
       (reverse)
       (get-ranks)
       (get-prefixes)
       (remove-duplicates)
       (rerank)))

(defn -main []
  (let [result (rank-movies "movies.csv")]
    (doseq [entry result]
      (println (format "rank: %2d, score: %2d, title: %s"
                       (entry :rank) (entry :score) (entry :title))))))
