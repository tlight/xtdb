(ns xtdb.operator.scan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
            [xtdb.buffer-pool :as bp]
            [xtdb.coalesce :as coalesce]
            [xtdb.expression :as expr]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.expression.walk :as expr.walk]
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.temporal :as temporal]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv]
            xtdb.watermark)
  (:import (clojure.lang MapEntry)
           (java.util ArrayList Map Set)
           (java.util.function Consumer)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector BigIntVector VectorSchemaRoot)
           (org.apache.arrow.vector BigIntVector VarBinaryVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.complex ListVector StructVector)
           (org.roaringbitmap IntConsumer RoaringBitmap)
           org.roaringbitmap.buffer.MutableRoaringBitmap
           xtdb.api.protocols.TransactionInstant
           xtdb.buffer_pool.IBufferPool
           xtdb.ICursor
           (xtdb.metadata IMetadataManager ITableMetadata)
           (xtdb.object_store ObjectStore)
           xtdb.operator.IRelationSelector
           (xtdb.vector IIndirectRelation IIndirectVector)
           (xtdb.watermark IWatermark IWatermarkSource)))

(s/def ::table symbol?)

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(defmethod lp/ra-expr :scan [_]
  (s/cat :op #{:scan}
         :scan-opts (s/keys :req-un [::table]
                            :opt-un [::lp/for-valid-time ::lp/for-system-time ::lp/default-all-valid-time?])
         :columns (s/coll-of (s/or :column ::lp/column
                                   :select ::lp/column-expression))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IScanEmitter
  (tableColNames [^xtdb.watermark.IWatermark wm, ^String table-name])
  (allTableColNames [^xtdb.watermark.IWatermark wm])
  (scanColTypes [^xtdb.watermark.IWatermark wm, scan-cols])
  (emitScan [scan-expr scan-col-types param-types]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->scan-cols [{:keys [columns], {:keys [table]} :scan-opts}]
  (for [[col-tag col-arg] columns]
    [table (case col-tag
             :column col-arg
             :select (key (first col-arg)))]))

(def ^:dynamic *column->pushdown-bloom* {})

(defn- filter-pushdown-bloom-block-idxs [^IMetadataManager metadata-manager chunk-idx ^String table-name ^String col-name ^RoaringBitmap block-idxs]
  (if-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
    ;; would prefer this `^long` to be on the param but can only have 4 params in a primitive hinted function in Clojure
    @(meta/with-metadata metadata-manager ^long chunk-idx table-name
       (util/->jfn
         (fn [^ITableMetadata table-metadata]
           (let [metadata-root (.metadataRoot table-metadata)
                 ^VarBinaryVector bloom-vec (-> ^ListVector (.getVector metadata-root "columns")
                                                ^StructVector (.getDataVector)
                                                (.getChild "bloom"))]
             (when (MutableRoaringBitmap/intersects pushdown-bloom
                                                    (bloom/bloom->bitmap bloom-vec (.rowIndex table-metadata col-name -1)))
               (let [filtered-block-idxs (RoaringBitmap.)]
                 (.forEach block-idxs
                           (reify IntConsumer
                             (accept [_ block-idx]
                               (when-let [bloom-vec-idx (.rowIndex table-metadata col-name block-idx)]
                                 (when (and (not (.isNull bloom-vec bloom-vec-idx))
                                            (MutableRoaringBitmap/intersects pushdown-bloom
                                                                             (bloom/bloom->bitmap bloom-vec bloom-vec-idx)))
                                   (.add filtered-block-idxs block-idx))))))

                 (when-not (.isEmpty filtered-block-idxs)
                   filtered-block-idxs)))))))
    block-idxs))

(defn- roaring-and
  (^org.roaringbitmap.RoaringBitmap [] (RoaringBitmap.))
  (^org.roaringbitmap.RoaringBitmap [^RoaringBitmap x] x)
  (^org.roaringbitmap.RoaringBitmap [^RoaringBitmap x ^RoaringBitmap y]
   (doto x
     (.and y))))

(defn- col-pred-selection ^longs [^BufferAllocator allocator, ^Map col-preds, ^IIndirectRelation in-rel, params]
  (->> (for [^IRelationSelector col-pred (vals col-preds)]
         (RoaringBitmap/bitmapOf (.select col-pred allocator in-rel params)))
       ^RoaringBitmap (reduce roaring-and (RoaringBitmap/bitmapOfRange 0 (.rowCount in-rel)))
       (.toArray)))

(defn- remove-col ^xtdb.vector.IIndirectRelation [^IIndirectRelation rel, ^String col-name]
  (iv/->indirect-rel (remove #(= col-name (.getName ^IIndirectVector %)) rel)
                     (.rowCount rel)))

(defn- unnormalize-column-names ^xtdb.vector.IIndirectRelation [^IIndirectRelation rel col-names]
  (iv/->indirect-rel
   (map (fn [col-name]
          (-> (.vectorForName ^IIndirectRelation rel (util/str->normal-form-str col-name))
              (.withName col-name)))
        col-names)))

(defn- ->t1-cursor ^xtdb.ICursor [^ObjectStore obj-store, ^IBufferPool buffer-pool
                                  ^String table-name, content-col-names, temporal-col-names
                                  ^longs _temporal-min-range, ^longs _temporal-max-range]
  ;; TODO Path rather than String in the OS/BP?
  (let [t1-directory-key (first (.listObjects obj-store (format "tables/%s/t1-directory" table-name)))
        block-idxs (RoaringBitmap.)]
    (with-open [^ArrowBuf buf @(.getBuffer buffer-pool t1-directory-key)
                chunks (util/->chunks buf)]
      (.tryAdvance chunks
                   (reify Consumer
                     (accept [_ dir-root]
                       ;; TODO use readers here
                       (let [^VectorSchemaRoot dir-root dir-root

                             node-trie-el-vec (-> ^ListVector (.getVector dir-root "node-trie")
                                                  ^DenseUnionVector (.getDataVector))

                             ^StructVector leaf-vec (.getVectorByType node-trie-el-vec 2)
                             ^BigIntVector page-idx-vec (.getChild leaf-vec "page-idx")]
                         (dotimes [n (.getValueCount node-trie-el-vec)]
                           (let [offset (.getOffset node-trie-el-vec n)]
                             ;; TODO check metadata
                             ;; TODO use temporal min/max range
                             ;; TODO `filter-pushdown-bloom-block-idxs`
                             ;; TODO limit to EID branches when we have an EID filter.
                             (case (.getTypeId node-trie-el-vec n)
                               2 (.add block-idxs (.get page-idx-vec offset))
                               nil))))))))

    (let [t1-file-name (last (str/split t1-directory-key #"/"))
          content-cursors (ArrayList.)]
      ;; TODO get the buffer-pool to give us slices instead - it might want to partially download them

      (try
        (doseq [col-name content-col-names]
          (.add content-cursors
                (-> @(.getBuffer buffer-pool (format "tables/%s/t1-content/%s/%s"
                                                     table-name col-name t1-file-name))
                    (util/->chunks {:block-idxs block-idxs, :close-buffer? true}))))

        (let [temporal-cursor (-> @(.getBuffer buffer-pool (format "tables/%s/t1-temporal/%s"
                                                                   table-name t1-file-name))
                                  (util/->chunks {:block-idxs block-idxs, :close-buffer? true}))]
          (try
            (let [cursor (util/combine-col-cursors (conj (vec content-cursors) temporal-cursor))]
              (reify ICursor
                (tryAdvance [_ c]
                  (.tryAdvance cursor
                               (reify Consumer
                                 (accept [_ in-root]
                                   (let [^VectorSchemaRoot in-root in-root]
                                     (.accept c (iv/->indirect-rel
                                                 (for [^String col-name (set/union content-col-names temporal-col-names)
                                                       :let [col-name (util/str->normal-form-str col-name)]]
                                                   (iv/->direct-vec (.getVector in-root col-name)))
                                                 (.getRowCount in-root))))))))
                (close [_]
                  (.close cursor))))

            (catch Throwable t
              (.close temporal-cursor)
              (throw t))))

        (catch Throwable t
          (run! util/try-close content-cursors)
          (throw t))))))

(deftype ScanCursor [^BufferAllocator allocator
                     ^Set col-names, ^Map col-preds
                     ^longs temporal-min-range
                     ^longs temporal-max-range
                     ^ICursor #_<IIR> blocks
                     params]
  ICursor
  (tryAdvance [_ c]
    (let [keep-row-id-col? (contains? col-names "_row_id")
          !advanced? (volatile! false)]

      (while (and (not @!advanced?)
                  (.tryAdvance blocks
                               (reify Consumer
                                 (accept [_ rel]
                                   (letfn [(accept-rel [^IIndirectRelation read-rel]
                                             (when (and read-rel (pos? (.rowCount read-rel)))
                                               (let [read-rel (cond-> read-rel
                                                                (not keep-row-id-col?) (remove-col "_row_id"))]
                                                 (.accept c read-rel)
                                                 (vreset! !advanced? true))))]
                                     (accept-rel (cond-> (-> rel
                                                             (unnormalize-column-names col-names))
                                                   (seq col-preds) (iv/select (col-pred-selection allocator col-preds rel params))))))))))
      (boolean @!advanced?)))

  (close [_]
    (util/try-close blocks)))

(defn ->temporal-min-max-range [^IIndirectRelation params, {^TransactionInstant basis-tx :tx}, {:keys [for-valid-time for-system-time]}, selects]
  (let [min-range (temporal/->min-range)
        max-range (temporal/->max-range)]
    (letfn [(apply-bound [f col-name ^long time-μs]
              (let [range-idx (temporal/->temporal-column-idx (util/str->normal-form-str (str col-name)))]
                (case f
                  :< (aset max-range range-idx
                           (min (dec time-μs) (aget max-range range-idx)))
                  :<= (aset max-range range-idx
                            (min time-μs (aget max-range range-idx)))
                  :> (aset min-range range-idx
                           (max (inc time-μs) (aget min-range range-idx)))
                  :>= (aset min-range range-idx
                            (max time-μs (aget min-range range-idx)))
                  nil)))

            (->time-μs [[tag arg]]
              (case tag
                :literal (-> arg
                             (util/sql-temporal->micros (.getZone expr/*clock*)))
                :param (-> (let [col (.vectorForName params (name arg))]
                             (types/get-object (.getVector col) (.getIndex col 0)))
                           (util/sql-temporal->micros (.getZone expr/*clock*)))
                :now (-> (.instant expr/*clock*)
                         (util/instant->micros))))]

      (when-let [system-time (some-> basis-tx (.system-time) util/instant->micros)]
        (apply-bound :<= "xt$system_from" system-time)

        (when-not for-system-time
          (apply-bound :> "xt$system_to" system-time)))

      (letfn [(apply-constraint [constraint start-col end-col]
                (when-let [[tag & args] constraint]
                  (case tag
                    :at (let [[at] args
                              at-μs (->time-μs at)]
                          (apply-bound :<= start-col at-μs)
                          (apply-bound :> end-col at-μs))

                    ;; overlaps [time-from time-to]
                    :in (let [[from to] args]
                          (apply-bound :> end-col (->time-μs (or from [:now])))
                          (when to
                            (apply-bound :< start-col (->time-μs to))))

                    :between (let [[from to] args]
                               (apply-bound :> end-col (->time-μs (or from [:now])))
                               (when to
                                 (apply-bound :<= start-col (->time-μs to))))

                    :all-time nil)))]

        (apply-constraint for-valid-time "xt$valid_from" "xt$valid_to")
        (apply-constraint for-system-time "xt$system_from" "xt$system_to"))

      (let [col-types (into {} (map (juxt first #(get temporal/temporal-col-types (util/str->normal-form-str (str (first %)))))) selects)
            param-types (expr/->param-types params)]
        (doseq [[col-name select-form] selects
                :when (temporal/temporal-column? (util/str->normal-form-str (str col-name)))]
          (->> (-> (expr/form->expr select-form {:param-types param-types, :col-types col-types})
                   (expr/prepare-expr)
                   (expr.meta/meta-expr {:col-types col-types}))
               (expr.walk/prewalk-expr
                (fn [{:keys [op] :as expr}]
                  (case op
                    :call (when (not= :or (:f expr))
                            expr)

                    :metadata-vp-call
                    (let [{:keys [f param-expr]} expr]
                      (when-let [v (if-let [[_ literal] (find param-expr :literal)]
                                     (when literal (->time-μs [:literal literal]))
                                     (->time-μs [:param (get param-expr :param)]))]
                        (apply-bound f col-name v)))

                    expr)))))
        [min-range max-range]))

    [min-range max-range]))

(defn tables-with-cols [basis ^IWatermarkSource wm-src ^IScanEmitter scan-emitter]
  (let [{:keys [tx, after-tx]} basis
        wm-tx (or tx after-tx)]
    (with-open [^IWatermark wm (.openWatermark wm-src wm-tx)]
      (.allTableColNames scan-emitter wm))))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :object-store (ig/ref :xtdb/object-store)
          :buffer-pool (ig/ref ::bp/buffer-pool)}))

;; TODO real col types
(def foo-tpch-col-types
  '{[customer c_acctbal] :f64, [customer c_address] :utf8, [customer c_comment] :utf8, [customer c_custkey] :utf8, [customer c_mktsegment] :utf8, [customer c_name] :utf8, [customer c_nationkey] :utf8, [customer c_phone] :utf8, [customer xt$id] :utf8,
    [lineitem l_comment] :utf8, [lineitem l_commitdate] [:date :day], [lineitem l_discount] :f64, [lineitem l_extendedprice] :f64, [lineitem l_linenumber] :i64, [lineitem l_linestatus] :utf8, [lineitem l_orderkey] :utf8, [lineitem l_partkey] :utf8, [lineitem l_quantity] :f64, [lineitem l_receiptdate] [:date :day], [lineitem l_returnflag] :utf8, [lineitem l_shipdate] [:date :day], [lineitem l_shipinstruct] :utf8, [lineitem l_shipmode] :utf8, [lineitem l_suppkey] :utf8, [lineitem l_tax] :f64, [lineitem xt$id] :utf8,
    [nation n_comment] :utf8, [nation n_name] :utf8, [nation n_nationkey] :utf8, [nation n_regionkey] :utf8, [nation xt$id] :utf8,
    [orders o_clerk] :utf8, [orders o_comment] :utf8, [orders o_custkey] :utf8, [orders o_orderdate] [:date :day], [orders o_orderkey] :utf8, [orders o_orderpriority] :utf8, [orders o_orderstatus] :utf8, [orders o_shippriority] :i64, [orders o_totalprice] :f64, [orders xt$id] :utf8,
    [part p_brand] :utf8 [part p_comment] :utf8, [part p_container] :utf8, [part p_mfgr] :utf8, [part p_name] :utf8, [part p_partkey] :utf8, [part p_retailprice] :f64, [part p_size] :i64, [part p_type] :utf8, [part xt$id] :utf8,
    [partsupp ps_availqty] :i64, [partsupp ps_comment] :utf8, [partsupp ps_partkey] :utf8, [partsupp ps_suppkey] :utf8, [partsupp ps_supplycost] :f64, [partsupp xt$id] :utf8,
    [region r_comment] :utf8, [region r_name] :utf8, [region r_regionkey] :utf8, [region xt$id] :utf8,
    [supplier s_acctbal] :f64, [supplier s_address] :utf8, [supplier s_comment] :utf8, [supplier s_name] :utf8, [supplier s_nationkey] :utf8, [supplier s_phone] :utf8, [supplier s_suppkey] :utf8, [supplier xt$id] :utf8,
    [xt$txs xt$committed?] :bool, [xt$txs xt$error] [:union #{:clj-form :null}], [xt$txs xt$id] :i64, [xt$txs xt$tx_time] [:timestamp-tz :micro "UTC"],
    })

(defmethod ig/init-key ::scan-emitter [_ {:keys [^IMetadataManager metadata-mgr
                                                 ^ObjectStore object-store
                                                 ^IBufferPool buffer-pool]}]
  (reify IScanEmitter
    (tableColNames [_ wm table-name]
      (let [normalized-table (util/str->normal-form-str table-name)]
        (into #{} cat [(keys (.columnTypes metadata-mgr normalized-table))
                       (some-> (.liveChunk wm)
                               (.liveTable normalized-table)
                               (.columnTypes)
                               keys)])))

    (allTableColNames [_ wm]
      (merge-with
       set/union
       (update-vals
        (.allColumnTypes metadata-mgr)
        (comp set keys))
       (update-vals
        (some-> (.liveChunk wm)
                (.allColumnTypes))
        (comp set keys))))

    (scanColTypes [_ wm scan-cols]
      (letfn [(->col-type [[table col-name]]
                (let [normalized-table (util/str->normal-form-str (str table))
                      normalized-col-name (util/str->normal-form-str (str col-name))]
                  (if (temporal/temporal-column? (util/str->normal-form-str (str col-name)))
                    [:timestamp-tz :micro "UTC"]
                    (types/merge-col-types (.columnType metadata-mgr normalized-table normalized-col-name)
                                           (some-> (.liveChunk wm)
                                                   (.liveTable normalized-table)
                                                   (.columnTypes)
                                                   (get normalized-col-name))))))]
        (->> scan-cols
             ;; TODO real scan col types
             (into {} (map (juxt identity #_->col-type foo-tpch-col-types))))))

    (emitScan [_ {:keys [columns], {:keys [table for-valid-time default-all-valid-time?] :as scan-opts} :scan-opts} scan-col-types param-types]
      (let [scan-opts (cond-> scan-opts
                        (nil? for-valid-time)
                        (assoc :for-valid-time (if default-all-valid-time? [:all-time] [:at [:now :now]])))

            col-names (->> columns
                           (into [] (comp (map (fn [[col-type arg]]
                                                 (case col-type
                                                   :column arg
                                                   :select (key (first arg)))))

                                          (distinct))))

            {content-col-names false, temporal-col-names true}
            (->> col-names (group-by (comp temporal/temporal-column? util/str->normal-form-str str)))

            content-col-names (-> (set (map str content-col-names))
                                  ;; TODO reinstate row-id
                                  #_(conj "_row_id"))
            #_#_
            normalized-content-col-names (set (map (comp util/str->normal-form-str) content-col-names))
            temporal-col-names (into #{} (map (comp str)) temporal-col-names)
            normalized-table-name (util/str->normal-form-str (str table))

            col-types (->> col-names
                           (into {} (map (juxt identity
                                               (fn [col-name]
                                                 (get scan-col-types [table col-name]))))))

            selects (->> (for [[tag arg] columns
                               :when (= tag :select)]
                           (first arg))
                         (into {}))

            col-preds (->> (for [[col-name select-form] selects]
                             ;; for temporal preds, we may not need to re-apply these if they can be represented as a temporal range.
                             (MapEntry/create (str col-name)
                                              (expr/->expression-relation-selector select-form {:col-types col-types, :param-types param-types})))
                           (into {}))

            #_#_
            metadata-args (vec (for [[col-name select] selects
                                     :when (not (temporal/temporal-column? (util/str->normal-form-str (str col-name))))]
                                 select))

            row-count (->> (meta/with-all-metadata metadata-mgr normalized-table-name
                             (util/->jbifn
                               (fn [_chunk-idx ^ITableMetadata table-metadata]
                                 (let [id-col-idx (.rowIndex table-metadata "xt$id" -1)
                                       ^BigIntVector count-vec (-> (.metadataRoot table-metadata)
                                                                   ^ListVector (.getVector "columns")
                                                                   ^StructVector (.getDataVector)
                                                                   (.getChild "count"))]
                                   (.get count-vec id-col-idx)))))
                           (reduce +))]

        {:col-types col-types
         :stats {:row-count row-count}
         :->cursor (fn [{:keys [allocator, ^IWatermark watermark, basis, params]}]
                     ;; TODO use metadata-pred
                     (let [ ; metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) col-types params)
                           [temporal-min-range temporal-max-range] (->temporal-min-max-range params basis scan-opts selects)]

                       #_  ; TODO use live chunk - here's what was here before
                       (util/->concat-cursor (->content-chunks allocator metadata-mgr buffer-pool
                                                               normalized-table-name normalized-content-col-names
                                                               metadata-pred)
                                             (some-> (.liveChunk watermark)
                                                     (.liveTable normalized-table-name)
                                                     (.liveBlocks normalized-content-col-names metadata-pred)))

                       (-> (ScanCursor. allocator
                                        (into #{} (map str) col-names) col-preds
                                        temporal-min-range temporal-max-range
                                        (->t1-cursor object-store buffer-pool normalized-table-name
                                                     content-col-names temporal-col-names
                                                     temporal-min-range temporal-max-range)
                                        params)
                           (coalesce/->coalescing-cursor allocator))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-col-types, param-types]}]
  (.emitScan scan-emitter scan-expr scan-col-types param-types))

(comment
  (do
    (require '[xtdb.node :as node]
             '[xtdb.test-util :as tu])



    (with-open [node (node/start-node {:xtdb.object-store/file-system-object-store {:root-path "/home/james/tmp/idx-poc/tpch-01"}})]
      (binding [tu/*allocator* (tu/component node :xtdb/allocator)]
        (tu/query-ra '[:assign [PartSupp [:join [{s_suppkey ps_suppkey}]
                                          [:join [{n_nationkey s_nationkey}]
                                           [:join [{n_regionkey r_regionkey}]
                                            [:scan {:for-valid-time [:at :now] :table nation} [n_name n_regionkey n_nationkey]]
                                            [:scan {:for-valid-time [:at :now] :table region} [r_regionkey {r_name (= r_name ?region)}]]]
                                           [:scan {:for-valid-time [:at :now] :table supplier} [s_nationkey s_suppkey s_acctbal s_name s_address s_phone s_comment]]]
                                          [:scan {:for-valid-time [:at :now] :table partsupp} [ps_suppkey ps_partkey ps_supplycost]]]]
                       [:top {:limit 100}
                        [:order-by [[s_acctbal {:direction :desc}] [n_name] [s_name] [p_partkey]]
                         [:project [s_acctbal s_name n_name p_partkey p_mfgr s_address s_phone s_comment]
                          [:join [{ps_partkey ps_partkey} {ps_supplycost min_ps_supplycost}]
                           [:join [{ps_partkey p_partkey}]
                            PartSupp
                            [:scan {:for-valid-time [:at :now] :table part} [p_partkey p_mfgr {p_size (= p_size ?size)} {p_type (like p_type "%BRASS")}]]]
                           [:group-by [ps_partkey {min_ps_supplycost (min ps_supplycost)}]
                            PartSupp]]]]]]
                     {:node node
                      :params {'?region "EUROPE"
                               ;; '?type "BRASS"
                               '?size 15}})))))
