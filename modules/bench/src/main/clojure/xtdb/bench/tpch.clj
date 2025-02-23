(ns xtdb.bench.tpch
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.bench.xtdb2 :as bxt]
            [xtdb.buffer-pool :as bp]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.query-ra :as query-ra]
            [xtdb.util :as util])
  (:import (java.time Duration InstantSource)
           (java.util AbstractMap)))

(defn bp-stats
  "Returns string reflecting buffer pool stats over a given test run"
  [ms]
  (let [miss-bytes (.get bp/cache-miss-byte-counter)
        hit-bytes (.get bp/cache-hit-byte-counter)
        ;; thanks swn: https://sw1nn.com/blog/2012/03/26/human-readable-size/
        humanize-bytes
        (fn [bytes]
          (let [unit 1024]
            (if (< bytes unit)
              (str bytes " B")
              (let [exp (int (/ (Math/log bytes)
                                (Math/log unit)))
                    pre (str (nth "KMGTPE" (dec exp)) "i")]
                (format "%.1f %sB" (/ bytes (Math/pow unit exp)) pre)))))

        humanize-nanos
        (fn [nanos]
          (let [nanos (long nanos)
                unit 1000]
            (if (< nanos unit)
              (str nanos " ns")
              (let [exp (min 3 (int (/ (Math/log nanos) (Math/log unit))))
                    suf (case exp
                          1 "μs"
                          2 "ms"
                          3 "s"
                          "s")]
                (format "%.1f %s" (/ nanos (Math/pow unit exp)) suf)))))

        cache-ratio (/ hit-bytes (max 1 (+ hit-bytes miss-bytes)))
        stall-nanos @bp/io-wait-nanos-counter]
    {"hit" (format "%s (%.2f)" (humanize-bytes hit-bytes) (double cache-ratio))
     "miss" (humanize-bytes miss-bytes)
     "io" (format "%s/sec" (humanize-bytes (/ miss-bytes (max 1 (/ ms 1000)))))
     "io-wait" (humanize-nanos stall-nanos)}))

(defn query-tpch [stage-name i]
  (let [q (nth tpch-ra/queries i)
        stage-name (keyword (str (name stage-name) "-" (:name (meta q))))
        q @q
        {::tpch-ra/keys [params table-args]} (meta q)]
    {:t :do
     :stage stage-name
     :tasks [{:t :call
              :f (fn [{:keys [sut]}]
                   (try
                     (count (query-ra/query-ra q {:node sut
                                                  :params params
                                                  :table-args table-args}))
                     (catch Exception e
                       (.printStackTrace e))))}]}))


(defn queries-stage [stage-name]
  {:t :do
   :stage stage-name
   :tasks (vec (concat [{:t :call :f (fn [{:keys [^AbstractMap custom-state]}]
                                       (bp/clear-cache-counters)
                                       (.put custom-state :bf-stats-start (System/currentTimeMillis)))}]

                       (for [i (range (count tpch-ra/queries))]
                         (query-tpch stage-name i))

                       [{:t :call :f (fn [{:keys [custom-state] :as worker}]
                                       (let [report-name (str (name stage-name) " buffer pool stats")
                                             start-ms (get custom-state :bf-stats-start)
                                             end-ms (System/currentTimeMillis)
                                             bf-stats (bp-stats (- end-ms start-ms))]
                                         (b/log-report worker {:stage report-name
                                                               :time-taken-ms (- end-ms start-ms)
                                                               :buffer-pool-stats bf-stats})))}]))})

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn benchmark [{:keys [scale-factor seed load-phase], :or {scale-factor 0.01, seed 0, load-phase true}}]
  (log/info {:scale-factor scale-factor})
  {:title "TPC-H (OLAP)"
   :seed seed
   :tasks
   [{:t :do
     :stage :ingest
     :tasks (into (if load-phase
                    [{:t :do
                      :stage :submit-docs
                      :tasks [{:t :call :f (fn [{:keys [sut]}] (tpch/submit-docs! sut scale-factor))}]}]
                    [])
                  [{:t :do
                    :stage :sync
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/sync-node sut (Duration/ofHours 5)))}]}
                   {:t :do
                    :stage :finish-chunk
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/finish-chunk! sut))}]}
                   {:t :do
                    :stage :compact
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/compact! sut))}]}])}

    (queries-stage :cold-queries)

    (queries-stage :hot-queries)]})

