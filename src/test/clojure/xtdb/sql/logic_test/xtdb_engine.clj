(ns xtdb.sql.logic-test.xtdb-engine
  (:require [clojure.string :as str]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            xtdb.node.impl
            [xtdb.rewrite :as r]
            [xtdb.sql.analyze :as sem]
            [xtdb.sql.logic-test.runner :as slt]
            [xtdb.sql.parser :as p]
            [xtdb.sql.plan :as plan]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.time Instant]
           [java.util HashMap UUID]
           xtdb.node.impl.Node))

;; TODO:
;; - needs cleanup.
;; - does not take renamed tables in presence of sub-queries into account.

(defn- normalize-rewrite [column->table tables]
  (letfn [(build-column-name-list [table tables]
            (vec
             (cons
              :column_name_list
              (for [col (get tables table)]
                [:regular_identifier col]))))]
    (fn [z]
      (r/zmatch z
        [:derived_column expr]
        ;;=>
        [:derived_column
         expr
         [:as_clause
          "AS"
          [:regular_identifier (str "col__" (r/child-idx z))]]]

        [:identifier_chain
         [:regular_identifier column]]
        ;;=>
        (when-let [table (get column->table column)]
          [:identifier_chain
           [:regular_identifier table]
           [:regular_identifier column]])

        [:sort_specification
         [:exact_numeric_literal ordinal]]
        ;;=>
        [:sort_specification
         [:column_reference
          [:identifier_chain
           [:regular_identifier (str "col__" ordinal)]]]]

        [:sort_specification
         [:exact_numeric_literal ordinal]
         ordering-spec]
        ;;=>
        [:sort_specification
         [:column_reference
          [:identifier_chain
           [:regular_identifier (str "col__" ordinal)]]]
         ordering-spec]

        [:sort_specification
         [:exact_numeric_literal ordinal]
         ordering-spec
         null-ordering]
        ;;=>
        [:sort_specification
         [:column_reference
          [:identifier_chain
           [:regular_identifier (str "col__" ordinal)]]]
         ordering-spec
         null-ordering]

        [:table_primary
         [:regular_identifier table]
         "AS"
         [:correlation_name [:regular_identifier table_alias]]]
        ;;=>
        [:table_primary
         [:regular_identifier table]
         "AS"
         [:correlation_name [:regular_identifier table_alias]]
         (build-column-name-list table tables)]

        [:table_primary
         [:regular_identifier table]
         [:correlation_name [:regular_identifier table_alias]]]
        ;;=>
        [:table_primary
         [:regular_identifier table]
         "AS"
         [:correlation_name [:regular_identifier table_alias]]
         (build-column-name-list table tables)]

        [:table_primary
         [:regular_identifier table]]
        ;;=>
        [:table_primary
         [:regular_identifier table]
         "AS"
         [:correlation_name [:regular_identifier table]]
         (build-column-name-list table tables)]))))

(defn- top-level-query-table->correlation-name [query]
  (->> (r/collect-stop
        (fn [z]
          (r/zmatch z
            [:table_primary [:regular_identifier table]]
            ;;=>
            [table table]

            [:table_primary [:regular_identifier table] "AS" [:correlation_name [:regular_identifier correlation-name]]]
            ;;=>
            [table correlation-name]

            [:table_primary [:regular_identifier table] [:correlation_name [:regular_identifier correlation-name]]]
            ;;=>
            [table correlation-name]

            (r/zcase z
              (:select_list
               :order_by_clause
               :where_clause
               :subquery)
              []
              nil)))
        (r/vector-zip query))
       (apply hash-map)))

(defn normalize-query [tables query]
  (let [table->correlation-name (top-level-query-table->correlation-name query)
        column->table (->> (for [[table columns] tables
                                 :when (contains? table->correlation-name table)
                                 column columns]
                             [column (get table->correlation-name table)])
                           (into {}))]
    (->> (r/vector-zip query)
         (r/topdown (r/adhoc-tp r/id-tp (normalize-rewrite column->table tables)))
         (r/node))))

(defn- create-table [node {:keys [table-name columns]}]
  (assert (nil? (get-in node [:tables table-name])))
  (assoc-in node [:tables table-name] columns))

(defn- create-view [node {:keys [view-name as]}]
  (assert (nil? (get-in node [:views view-name])))
  (assoc-in node [:views view-name] as))

(defn- execute-record [node record]
  (case (:type record)
    :create-table (create-table node record)
    :create-view (create-view node record)))

(defn outer-projection [tree]
  ;;TODO no support for select * in SLT, outer-projection would require access to table-info
  (->> (sem/projected-columns (r/$ (r/vector-zip tree) 1))
       (first)
       (mapv (comp util/symbol->normal-form-symbol #(or (:outer-name %) (plan/unqualified-projection-symbol %))))))

(defn execute-query-expression [node from-subquery]
  (binding [r/*memo* (HashMap.)]
    (let [tree (normalize-query (:tables node) from-subquery)
          projection (outer-projection tree)
          plan (-> tree
                   (sem/analyze-query) sem/or-throw
                   (plan/plan-query {:decorrelate? true, :project-anonymous-columns? true}))
          column->anonymous-col (:column->name (meta plan))]
      (vec (for [row (tu/query-ra plan {:node node})]
             (mapv #(-> (get column->anonymous-col %) name keyword row) projection))))))

(defn insert->docs [{:keys [tables] :as node} insert-statement]
  (let [[_ _ _ insertion-target insert-columns-and-source] insert-statement
        table (first (filter string? (flatten insertion-target)))
        from-subquery insert-columns-and-source
        from-subquery-results (execute-query-expression node (last from-subquery))
        columns (if (= 1 (count (rest from-subquery)))
                  (get tables table)
                  (let [insert-column-list (second from-subquery)]
                    (->> (flatten insert-column-list)
                         (filter string?))))]
    (for [row from-subquery-results]
      (-> (zipmap (map keyword columns) row)
          (with-meta {:table (keyword table)})))))

(defn- insert-statement [node insert-statement]
  (xt/submit-tx node (vec (for [doc (insert->docs node insert-statement)]
                            [:put-docs (:table (meta doc))
                             (merge {:xt/id (UUID/randomUUID)} doc)]))
                {:default-all-valid-time? true})
  node)

(defn skip-statement? [^String x]
  (boolean (re-find #"(?is)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.+)\)\s*$" x)))

(defn- execute-statement [node direct-sql-data-statement]
  (binding [r/*memo* (HashMap.)]
    (case (first direct-sql-data-statement)
      :insert_statement (insert-statement node direct-sql-data-statement))))

(defn- execute-sql-statement [node sql-statement variables opts]
  (binding [r/*memo* (HashMap.)]
    (xt/submit-tx node
                  [[:sql sql-statement]]
                  (-> opts
                      (assoc :default-all-valid-time? (not= (get variables "VALID_TIME_DEFAULTS") "AS_OF_NOW"))
                      (cond-> (get variables "CURRENT_TIMESTAMP") (assoc-in [:basis :current-time] (Instant/parse (get variables "CURRENT_TIMESTAMP"))))))
    node))

(defn- execute-sql-query [node sql-statement variables opts]
  (binding [r/*memo* (HashMap.)]
    (let [projection (mapv str (outer-projection (p/parse sql-statement :query_expression)))]
      (vec
       (for [row (xt/q node sql-statement
                       (-> opts
                           (assoc :key-fn :snake-case-string)
                           (assoc :default-all-valid-time? (not= (get variables "VALID_TIME_DEFAULTS") "AS_OF_NOW"))
                           (cond-> (get variables "CURRENT_TIMESTAMP") (assoc-in [:basis :current-time] (Instant/parse (get variables "CURRENT_TIMESTAMP"))))))]

         (mapv row projection))))))

(defn parse-create-table [^String x]
  (when-let [[_ table-name columns] (re-find #"(?is)^\s*CREATE\s+TABLE\s+(\w+)\s*\((.+)\)\s*$" x)]
    {:type :create-table
     :table-name table-name
     :columns (vec (for [column (str/split columns #",")]
                     (->> (str/split column #"\s+")
                          (remove str/blank?)
                          (first))))}))

(defn parse-create-view [^String x]
  (when-let [[_ view-name query] (re-find #"(?is)^\s*CREATE\s+VIEW\s+(\w+)\s+AS\s+(.+)\s*$" x)]
    {:type :create-view
     :view-name view-name
     :as query}))

(defn preprocess-query [^String query]
  (let [query (str/replace query #"(FROM\s+?)\(((?:[^(])+?CROSS\s+JOIN.+?)\)" "$1$2")
        query (str/replace query "CROSS JOIN" ",")]
    (if (re-find #"(?i)( FROM |^\s*VALUES)" query)
      query
      (str query " FROM (VALUES (0)) AS no_from"))))

(extend-protocol slt/DbEngine
  Node
  (get-engine-name [_] "xtdb")

  (execute-statement [this statement variables]
    (if (skip-statement? statement)
      this
      (if (:direct-sql slt/*opts*)
        (execute-sql-statement this statement variables (select-keys slt/*opts* [:decorrelate?]))
        (let [tree (p/parse statement :directly_executable_statement)]
          (if (p/failure? tree)
            (if-let [record (or (parse-create-table statement)
                                (parse-create-view statement))]
              (execute-record this record)
              (throw (err/illegal-arg :xtdb.sql/parse-error
                                      {::err/message (p/failure->str tree)
                                       :statement statement})))
            (let [direct-sql-data-statement-tree (second tree)]
              (execute-statement this direct-sql-data-statement-tree)))))))

  (execute-query [this query variables]
    (if (:direct-sql slt/*opts*)
      (execute-sql-query this query variables (select-keys slt/*opts* [:decorrelate?]))
      (let [edited-query (preprocess-query query)
            tree (p/parse edited-query :query_expression)]
        (when (p/failure? tree)
          (throw (err/illegal-arg :xtdb.sql/parse-error
                                  {::err/message (p/failure->str tree)
                                   :query query})))
        (execute-query-expression this tree)))))
