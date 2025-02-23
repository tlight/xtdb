(ns xtdb.sql.analyze
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.information-schema :as info-schema]
            [xtdb.rewrite :as r]
            [xtdb.sql.parser :as p]
            [xtdb.util :as util]))

(defn- ->line-info-str [loc]
  (let [{:keys [sql]} (meta (r/root loc))
        {:keys [start-idx]} (meta (r/node loc))
        {:keys [line column]} (p/index->line-column sql start-idx)]
    (format "at line %d, column %d" line column)))

(defn- ->src-str [loc]
  (let [{:keys [sql]} (meta (r/root loc))
        {:keys [start-idx end-idx]} (meta (r/node loc))]
    (str/trim (subs sql start-idx end-idx))))

;; Attribute grammar for SQL semantics.

;; TODO:
;; - try replace ids with refs.
;; - align names and language with spec, add references?
;; - named columns join should only output single named columns: COALESCE(lhs.x, rhs.x) AS x

(def ^:dynamic *table-info* nil)

(defn- enter-env-scope
  ([env]
   (enter-env-scope env {}))
  ([env new-scope]
   (cons new-scope env)))

(defn- update-env [[s & ss :as env] f]
  (cons (f s) ss))

(defn- extend-env [env k v]
  (update-env env (fn [s]
                    (update s k conj v))))

(defn local-env [[s]]
  s)

(defn local-env-singleton-values [[s]]
  (->> (for [[k [v]] s]
         [k v])
       (into {})))

(defn- parent-env [[_ & ss]]
  ss)

(defn- find-local-decl [[s & ss :as env] k]
  (when s
    (first (get s k))))

(defn find-decl [[s & ss :as env] k]
  (when s
    (or (find-local-decl env k)
        (recur ss k))))

(defn- qualified? [ids]
  (> (count ids) 1))

;; Attributes

(declare cte-env env projected-columns scope-element subquery-element)

;; Ids

(defn- prev-subtree-seq [ag]
  (when ag
    (lazy-seq (cons ag (prev-subtree-seq (r/zprev-no-edit ag))))))

(defn id [ag]
  (dec (count (prev-subtree-seq ag))))

(defn dynamic-param-idx [ag]
  (->> (prev-subtree-seq ag)
       (filter (partial r/ctor? :dynamic_parameter_specification))
       (count)
       (dec)))

(defn param-count [ag]
  (apply max
         (count (->> ag
                     (r/collect
                      (fn [ag]
                        (if (r/ctor? :dynamic_parameter_specification ag)
                          [:param]
                          [])))))
         (->> ag
              (r/collect (fn [ag]
                           (r/zmatch ag
                             [:postgres_parameter_specification s] [(parse-long (subs s 1))]))))))

;; Identifiers

(defn identifier [ag]
  (r/zcase ag
    :derived_column
    (r/zmatch ag
      [:derived_column _ [:as_clause _ ^:z ident]]
      ;;=>
      (identifier ident)

      [:derived_column _ [:as_clause ^:z ident]]
      ;;=>
      (identifier ident)

      [:derived_column
       [:column_reference
        [:identifier_chain _ ^:z ident]]]
      ;;=>
      (identifier ident)

      [:derived_column
       [:column_reference
        [:schema_name _]
        [:identifier_chain _ ^:z ident]]]
      ;;=>
      (identifier ident)

      [:derived_column
       [:field_reference _ ^:z ident]]
      ;;=>
      (identifier ident)

      [:derived_column
       [:subquery ^:z sq]]
      ;;=>
      (:identifier (ffirst (projected-columns sq)))

      [:derived_column
       [:host_parameter_name column]]
      ;;=>
      column)

    :as_clause
    (identifier (r/$ ag -1))

    :regular_identifier
    (util/str->normal-form-str (r/lexeme ag 1))

    :delimited_identifier
    (let [lexeme (r/lexeme ag 1)]
      (util/str->normal-form-str (subs lexeme 1 (dec (count lexeme)))))

    :correlation_name
    (identifier (r/$ ag 1))

    nil))

(defn identifiers [ag]
  (r/zcase ag
    (:column_name_list
     :column_reference
     :asterisked_identifier_chain
     :identifier_chain)
    (r/collect
     (fn [ag]
       (r/zcase ag
         (:regular_identifier :delimited_identifier) [(identifier ag)]
         nil))
     ag)))

(defn- table-or-query-name [ag]
  (r/zcase ag
    (:table_primary
     :with_list_element
     :target_table)
    (table-or-query-name (r/$ ag 1))

    :schema_name
    (table-or-query-name (r/right ag))

    :regular_identifier
    (identifier ag)

    :delimited_identifier
    (identifier ag)

    (:delete_statement__searched :update_statement__searched :erase_statement__searched)
    (table-or-query-name (r/find-first (partial r/ctor? :target_table) ag))

    nil))

(defn- correlation-name [ag]
  (r/zcase ag
    :table_primary
    (or (correlation-name (r/$ ag -1))
        (correlation-name (r/$ ag -2)))

    :correlation_name
    (identifier ag)

    :target_table
    (correlation-name (r/parent ag))

    (:delete_statement__searched :update_statement__searched :erase_statement__searched)
    (some-> (r/find-first (partial r/ctor? :correlation_name) ag)
            (identifier))

    nil))

;; With

(defn- cte [ag]
  (r/zcase ag
    :with_list_element
    (let [columns (when (r/ctor? :column_name_list (r/$ ag 2))
                    (identifiers (r/$ ag 2)))]
      (with-meta
        (cond-> {:query-name (table-or-query-name ag)
                 :id (id ag)
                 :scope-id (id (scope-element ag))
                 :subquery-scope-id (id (subquery-element (r/$ ag -1)))}
          columns (assoc :columns columns))
        {:ref ag}))))

;; Inherited
(defn ctei [ag]
  (r/zcase ag
    :query_expression
    (enter-env-scope (cte-env (r/parent ag)))

    :with_list_element
    (let [cte-env (ctei (r/left-or-parent ag))
          {:keys [query-name] :as cte} (cte ag)]
      (extend-env cte-env query-name cte))

    ::r/inherit))

;; Synthesised
(defn- cteo [ag]
  (r/zcase ag
    :query_expression
    (if (r/ctor? :with_clause (r/$ ag 1))
      (cteo (r/$ ag 1))
      (ctei ag))

    :with_clause
    (cteo (r/$ ag -1))

    :with_list
    (ctei (r/$ ag -1))))

(defn cte-env [ag]
  (r/zcase ag
    :query_expression
    (cteo ag)

    :with_list_element
    (let [with-clause (r/parent (r/parent ag))]
      (if (= "RECURSIVE" (r/lexeme with-clause 2))
        (cteo (r/parent ag))
        (ctei (r/left-or-parent ag))))

    ::r/inherit))

;; From

(defn derived-columns [ag]
  (r/zcase ag
    :table_primary
    (when (r/ctor? :column_name_list (r/$ ag -1))
      (identifiers (r/$ ag -1)))))

(defn subquery-element [ag]
  (r/zcase ag
    (:table_primary
     :subquery)
    (subquery-element (r/$ ag 1))

    :lateral_derived_table
    (subquery-element (r/$ ag 2))

    (:query_expression
     :collection_derived_table)
    ag

    nil))

(defn schema [ag]
  (r/zcase ag
    :table_primary
    (schema (r/$ ag 1))
    :schema_name
    (r/lexeme ag 1)
    nil))

(defn table [ag]
  (r/zcase ag
    :table_primary
    (when-not (r/ctor? :qualified_join (r/$ ag 1))
      (let [table-name (table-or-query-name ag)
            correlation-name (or (correlation-name ag) table-name)
            cte (when table-name
                  (find-decl (cte-env ag) table-name))
            sq-element (when (nil? cte)
                         (subquery-element ag))
            sq-scope-id (when (and sq-element (not= :collection_derived_table (r/ctor sq-element)))
                          (id sq-element))
            derived-columns (or (derived-columns ag) (:columns cte))
            {:keys [known-columns schema]}
            (let [schema (schema ag)]
              (case schema
                "INFORMATION_SCHEMA"
                {:schema schema
                 :known-columns (get info-schema/info-table-cols table-name)}
                "PG_CATALOG"
                {:schema schema
                 :known-columns (get info-schema/pg-catalog-table-cols table-name)}
                "PUBLIC"
                {:schema schema
                 :known-columns (get *table-info* table-name)}
                (cond (get *table-info* table-name)
                      {:schema "PUBLIC"
                       :known-columns (get *table-info* table-name)}
                      (get info-schema/pg-catalog-table-cols table-name)
                      {:schema "PG_CATALOG"
                       :known-columns (get info-schema/pg-catalog-table-cols table-name)}
                      :else
                      {:schema "PUBLIC"
                       :known-columns #{}})))]


        (with-meta
          (cond-> {:correlation-name correlation-name
                   :id (id ag)
                   :scope-id (id (scope-element ag))
                   :schema schema}
            table-name (assoc :table-or-query-name table-name)
            derived-columns (assoc :derived-columns derived-columns)
            sq-scope-id (assoc :subquery-scope-id sq-scope-id)
            cte (assoc :cte-id (:id cte) :cte-scope-id (:scope-id cte))
            known-columns (assoc :known-columns known-columns))
          (cond-> {:ref ag}
            sq-element (assoc :subquery-ref sq-element)
            cte (assoc :cte cte
                       :subquery-ref (subquery-element (r/$ (:ref (meta cte)) -1)))))))

    :target_table
    (let [table-name (table-or-query-name ag)]
      (-> {:correlation-name (or (correlation-name ag) table-name)
           :id (id ag)
           :scope-id (id (scope-element ag))
           :table-or-query-name table-name
           :known-columns (or (get *table-info* table-name)
                              (get info-schema/info-table-cols table-name))}
          (with-meta {:ref ag})))

    (:delete_statement__searched :update_statement__searched :erase_statement__searched)
    (table (r/find-first (partial r/ctor? :target_table) ag))

    (:query_valid_time_period_specification :query_system_time_period_specification)
    (table (r/parent ag))))

(defn local-tables [ag]
  (r/collect-stop
   (fn [ag]
     (r/zcase ag
       :target_table
       [(table ag)]

       :table_primary
       (if (r/ctor? :qualified_join (r/$ ag 1))
         (local-tables (r/$ ag 1))
         [(table ag)])

       :subquery []

       nil))
   ag))

(defn join-type [ag]
  (if (r/ctor? :qualified_join ag)
    (join-type (r/$ ag 2))
    (r/zmatch ag
      [:join_type [:outer_join_type ojt]] ojt
      [:join_type [:outer_join_type ojt] "OUTER"] ojt
      [:join_type "INNER"] "INNER"
      "JOIN" "INNER"
      nil)))

(defn- named-columns-join-columns [ag]
  (r/zcase ag
    :qualified_join
    (named-columns-join-columns (r/$ ag -1))

    :named_columns_join
    (identifiers (r/$ ag -1))

    nil))

(defn named-columns-join-env [ag]
  (r/zcase ag
    :qualified_join
    (named-columns-join-env (r/$ ag -1))

    :join_condition
    nil

    :named_columns_join
    (let [join-columns (identifiers (r/$ ag -1))
          qualified-join (r/parent ag)
          lhs (r/$ qualified-join 1)
          rhs (r/$ qualified-join -2)
          [lhs rhs] (for [side [lhs rhs]]
                      (select-keys
                       (->> (for [table (local-tables side)
                                  projection (first (projected-columns (:ref (meta table))))]
                              projection)
                            (group-by :identifier))
                       join-columns))]
      {:join-columns join-columns
       :lhs lhs
       :rhs rhs})

    ::r/inherit))

;; Inherited
(defn dcli [ag]
  (r/zcase ag
    (:query_specification :delete_statement__searched :update_statement__searched :erase_statement__searched)
    (enter-env-scope (env (r/parent ag)))

    (:table_primary :qualified_join :target_table)
    (reduce (fn [acc {:keys [correlation-name] :as table}]
              (extend-env acc correlation-name table))
            (dcli (r/left-or-parent ag))
            (local-tables ag))

    ::r/inherit))

;; Synthesised
(defn- dclo [ag]
  (r/zcase ag
    :query_specification
    (dclo (r/$ ag -1))

    (:delete_statement__searched :update_statement__searched :erase_statement__searched)
    (dclo (r/find-first (partial r/ctor? :target_table) ag))

    :table_expression
    (dclo (r/$ ag 1))

    :from_clause
    (dclo (r/$ ag 2))

    :table_reference_list
    (dcli (r/$ ag -1))

    :target_table
    (dcli ag)))

(defn env [ag]
  (r/zcase ag
    (:query_specification :delete_statement__searched :update_statement__searched :erase_statement__searched)
    (dclo ag)

    :from_clause
    (parent-env (dclo ag))

    (:collection_derived_table :join_condition :lateral_derived_table)
    (dcli (r/parent ag))

    :order_by_clause
    (let [query-expression-body (r/left ag)]
      (env query-expression-body))

    ::r/inherit))

;; Group by

(defn- grouping-column-references [ag]
  (r/collect
   (fn [ag]
     (when (r/ctor? :column_reference ag)
       [(identifiers ag)]))
   ag))

(defn- grouping-columns [ag]
  (->> (r/collect-stop
        (fn [ag]
          (r/zcase ag
            (:aggregate_function
             :having_clause)
            [[]]

            :group_by_clause
            [(grouping-column-references ag)]

            (:subquery
             :from_clause
             :order_by_clause)
            []

            nil))
        ag)
       (sort-by count)
       (last)))

(defn group-env [ag]
  (r/zcase ag
    :query_specification
    (enter-env-scope (group-env (r/parent ag))
                     {:grouping-columns (grouping-columns ag)
                      :group-column-reference-type :ordinary
                      :column-reference-type :ordinary})
    (:select_list
     :having_clause)
    (let [group-env (group-env (r/parent ag))
          {:keys [grouping-columns]} (local-env group-env)]
      (cond-> group-env
        grouping-columns (update-env (fn [s]
                                       (assoc s
                                              :group-column-reference-type :group-invariant
                                              :column-reference-type :invalid-group-invariant)))))

    :aggregate_function
    (update-env (group-env (r/parent ag))
                (fn [s]
                  (assoc s :column-reference-type :within-group-varying)))

    ::r/inherit))

;; Select

(defn- set-operator [ag]
  (r/zcase ag
    :query_expression_body
    (case (r/lexeme ag 2)
      "UNION" "UNION"
      "EXCEPT" "EXCEPT"
      nil)

    :query_term
    (case (r/lexeme ag 2)
      "INTERSECT" "INTERSECT"
      nil)

    nil))

(defn dml-app-time-extents [ag]
  (when-let [atpn (r/find-first (partial r/ctor? :valid_time_period_name) ag)]
    (if (or (= (r/znode (r/left atpn)) "ALL")
            (= (r/znode (r/right atpn)) "ALL"))
      :all-application-time
      (let [from (-> atpn r/right r/right)]
        {:from from
         :to (-> from r/right r/right)}))))

(defn expand-underlying-column-references [col-ref]
  (case (:type col-ref)
    :system-time-period-reference
    [(update col-ref :identifiers #(vector (first %) "xt$system_from"))
     (update col-ref :identifiers #(vector (first %) "xt$system_to"))]
    :application-time-period-reference
    [(update col-ref :identifiers #(vector (first %) "xt$valid_from"))
     (update col-ref :identifiers #(vector (first %) "xt$valid_to"))]
    [col-ref]))

(declare column-reference)

(defn all-column-references [ag]
  (->> (r/collect
         (fn [ag]
           (when (r/ctor? :column_reference ag)
             [(column-reference ag)]))
         ag)
       (mapcat expand-underlying-column-references)))

(defn all-select-asterisk-references [ag]
  (r/collect-stop
   (fn [ag]
     (r/zcase ag
       :select_list
       (r/collect-stop
        (fn [ag]
          (r/zcase ag
            :qualified_asterisk
            (identifiers (r/$ ag 1))
            :asterisk
            ["*"]
            :subquery []
            nil))
        ag)
       :subquery []
       nil))
   ag))

(defn generate-unique-column-names [projections]
  (->> projections
       (reduce
         (fn try-use-unique-col-name [{:keys [acc ret]} {:keys [identifier] :as projection}]
           (if identifier
             (if-let [col-name-count (get acc identifier)]
               {:acc (assoc acc identifier (inc col-name-count))
                :ret (conj ret (assoc projection :outer-name (symbol (str (name identifier) ":" (inc col-name-count)))))}
               {:acc (assoc acc identifier 0)
                :ret (conj ret projection)})
             {:acc acc
              :ret (conj ret projection)}))
         {:acc {}
          :ret []})
         (:ret)))

(defn- update-set-cols [ag]
  (some->> (r/find-first (partial r/ctor? :set_clause_list) ag)
           (r/collect-stop
            (fn [ag]
              (r/zmatch ag
                [:set_clause [:update_target ^:z id] _ ^:z us]
                [(-> {:identifier (identifier id)}
                     (vary-meta assoc :ref us))])))))

(defn expand-asterisk [ag]
  (r/zcase ag
    :table_primary
    (let [{:keys [known-columns correlation-name derived-columns] :as table} (table ag)
          projections (if-let [derived-columns (not-empty derived-columns)]
                        (for [identifier derived-columns]
                          {:identifier identifier})
                        (if-let [subquery-ref (:subquery-ref (meta table))]
                          (first (projected-columns subquery-ref))
                          (map #(hash-map :identifier %) known-columns)))
          grouping-columns (:grouping-columns (local-env (group-env ag)))
          grouping-columns-set (set grouping-columns)]
      (->>
       projections
       (map-indexed
        (fn [idx {:keys [identifier index outer-name]}]
          (cond-> (with-meta {:index idx} {:table table})
            identifier (assoc :identifier identifier)
            (and index (nil? identifier)) (assoc :original-index index)
            correlation-name (assoc :qualified-column [correlation-name identifier])
            outer-name (assoc :inner-name outer-name))))
       (map #(if (and grouping-columns
                      (not (contains? grouping-columns-set (:qualified-column %))))
               ;; TODO to be spec compliant this should really be as test for functional
               ;; dependency given our current lack of schema/index/unique constraints
               ;; this could prove hard, but we could explicitly look for xt/id
               (assoc % :invalid-ref-to-non-grouping-col true)
               %))
       (seq))) ;;an empty projection should only happen in cases of nested table primaries,
               ;; such as parenthesized-joined-tables.

    :subquery
    []

    nil))

(defn projected-columns
  "Returns a vector of candidates for the projected-cols of an expression.

   Most of the time, this is a singleton vector (hence `(first (sem/projected-columns z))` everywhere)
   but there are multiple candidates in set operators."
  [ag]
  (r/zcase ag
    :table_primary
    (when-not (r/ctor? :qualified_join (r/$ ag 1))
      (let [{:keys [correlation-name derived-columns], table-id :id, :as table} (table ag)
            projections (if-let [derived-columns (not-empty derived-columns)]
                          (for [identifier derived-columns]
                            {:identifier identifier})
                          (if-let [subquery-ref (:subquery-ref (meta table))]
                            (first (projected-columns subquery-ref))
                            (let [query-specification (scope-element ag)
                                  query-expression (scope-element (r/parent query-specification))
                                  named-join-columns (for [identifier (named-columns-join-columns (r/parent ag))]
                                                       {:identifier identifier})
                                  column-references (all-column-references query-expression)
                                  select-asterisk-references (set (all-select-asterisk-references query-specification))
                                  columns-from-asterisk (if (or (contains? select-asterisk-references "*")
                                                                (contains? select-asterisk-references correlation-name))
                                                          (map #(select-keys % [:identifier]) (expand-asterisk ag))
                                                          [])]
                              (->> (for [{:keys [identifiers] column-table-id :table-id} column-references
                                         :when (= table-id column-table-id)]
                                     {:identifier (last identifiers)})
                                   (concat named-join-columns)
                                   (concat columns-from-asterisk)
                                   (distinct)))))]
        [(map-indexed
          (fn [idx {:keys [identifier index outer-name]}]
            (cond-> (with-meta {:index idx} {:table table})
              identifier (assoc :identifier identifier)
              (and index (nil? identifier)) (assoc :original-index index)
              correlation-name (assoc :qualified-column [correlation-name identifier])
              outer-name (assoc :inner-name outer-name)))
          projections)]))

    :query_specification
    (letfn [(calculate-select-list [ag]
              (r/zcase ag
                :asterisk
                (let [table-expression (r/right (r/parent ag))]
                  (r/collect-stop expand-asterisk table-expression))

                :qualified_asterisk
                (let [identifiers (identifiers (r/$ ag 1))
                      table-expression (r/right (r/parent ag))]
                  (for [{:keys [qualified-column] :as projection} (r/collect-stop expand-asterisk table-expression)
                        :when (= identifiers (butlast qualified-column))]
                    projection))

                :derived_column
                (let [identifier (identifier ag)
                      qualified-column (when (r/ctor? :column_reference (r/$ ag 1))
                                         (identifiers (r/$ ag 1)))]

                  [(with-meta
                     (cond-> {:normal-form (r/node (r/$ ag 1))}
                       identifier (assoc :identifier identifier)
                       qualified-column (assoc :qualified-column qualified-column))
                     {:ref ag})])

                nil))]
      (let [sl (r/$ ag -2)
            projections (r/collect-stop calculate-select-list sl)]
        [(->> projections
              (generate-unique-column-names)
              (map-indexed
               (fn [idx projection]
                 (assoc projection :index idx))))]))

    :query_expression
    (let [query-expression-body (if (r/ctor? :with_clause (r/$ ag 1))
                                  (r/$ ag 2)
                                  (r/$ ag 1))
          keys-to-keep (if (r/ctor? :query_specification query-expression-body)
                         [:identifier :index :normal-form :outer-name]
                         [:identifier :index :outer-name])
          projs (projected-columns query-expression-body)]

      (vec (for [projections projs]
             (vec (for [projection projections]
                    (select-keys projection keys-to-keep))))))

    :target_table
    [(let [{:keys [correlation-name known-columns], table-id :id, :as table} (table ag)
           column-references (all-column-references (r/parent ag))
           overwrite-cols (update-set-cols (r/parent ag))]
       (->> (concat (for [{:keys [identifiers], column-table-id :table-id} column-references
                          :when (= table-id column-table-id)]
                      (last identifiers))

                    #{"xt$iid" "xt$valid_from" "xt$valid_to" "xt$system_from" "xt$system_to"}

                    (when (= :update_statement__searched (r/ctor (r/parent ag)))
                      (set/difference known-columns
                                      (into #{} (map :identifier) overwrite-cols))))
            (into [] (comp (distinct)
                           (map (fn [col-name]
                                  (-> {:identifier col-name
                                       :qualified-column [correlation-name col-name]}
                                      (vary-meta assoc :table table))))))))]

    :insert_statement
    (projected-columns (r/$ ag -1))

    :from_subquery
    (r/zmatch ag
      [:from_subquery ^:z query-expression]
      (projected-columns query-expression)

      [:from_subquery ^:z column-name-list _qe]
      (projected-columns column-name-list))

    :column_name_list
    [(vec (for [ident (identifiers ag)]
            {:identifier ident}))]

    (:update_statement__searched :delete_statement__searched)
    [(let [{:keys [correlation-name known-columns], :as table} (table ag)
           updated-cols (update-set-cols ag)]
       (vec
        (concat (->> (for [col-name (set/union #{"xt$iid" "xt$valid_from" "xt$valid_to" "xt$system_from" "xt$system_to"}
                                               (when (= :update_statement__searched (r/ctor ag))
                                                 (set/difference known-columns
                                                                 (into #{} (map :identifier) updated-cols))))]
                       {:identifier col-name
                        :qualified-column [correlation-name col-name]})
                     (into [] (map #(vary-meta % assoc :table table))))
                updated-cols)))]

    :erase_statement__searched
    [(let [{:keys [correlation-name], :as table} (table ag)]
       (->> (for [col-name ["xt$iid" "xt$valid_from" "xt$valid_to" "xt$system_from" "xt$system_to"]]
              {:identifier col-name
               :qualified-column [correlation-name col-name]})
            (into [] (map #(vary-meta % assoc :table table)))))]

    :collection_derived_table
    (if (= "ORDINALITY" (r/lexeme ag -1))
      [[{:index 0} {:index 1}]]
      [[{:index 0}]])

    :table_value_constructor
    (projected-columns (r/$ ag 2))

    (:row_value_expression_list :contextually_typed_row_value_expression_list :in_value_list)
    (r/collect-stop
     (fn [ag]
       (r/zcase ag
         (:row_value_expression_list
          :contextually_typed_row_value_expression_list)
         nil

         (:explicit_row_value_constructor
          :contextually_typed_row_value_constructor)
         (let [degree (r/collect-stop
                       (fn [ag]
                         (r/zcase ag
                           (:row_value_constructor_element
                            :contextually_typed_row_value_constructor_element)
                           1

                           :subquery
                           0

                           nil))
                       +
                       ag)]
           [(vec (for [n (range degree)]
                   {:index n}))])


         :subquery
         (projected-columns (r/$ ag 1))

         (when (r/ctor ag)
           [[{:index 0}]])))
     ag)

    (:query_expression_body
     :query_term)
    (r/collect-stop
     (fn [ag]
       (r/zcase ag
         (:query_specification
          :table_value_constructor)
         (projected-columns ag)

         :subquery
         []

         nil))
     ag)

    :subquery
    (projected-columns (r/$ ag 1))

    ::r/inherit))

;; Order by

(defn order-by-index [ag]
  (r/zcase ag
    :query_expression
    nil

    :sort_specification
    (first (for [{:keys [normal-form index identifier]} (first (projected-columns ag))
                 :when (or (= normal-form (r/lexeme ag 1))
                           (and (r/ctor? :column_reference (r/$ ag 1))
                                (= identifier (str/join "." (identifiers (r/$ ag 1))))))]
             index))

    ::r/inherit))

(defn order-by-indexes [ag]
  (r/zcase ag
    :query_expression
    (r/collect-stop
     (fn [ag]
       (r/zcase ag
         :sort_specification
         [(order-by-index ag)]

         :subquery
         []

         nil))
     ag)

    ::r/inherit))

;; Column references

(defn column-reference [ag]
  (r/zcase ag
    :column_reference
    (let [identifiers (identifiers ag)
          schema (schema (r/$ ag 1))
          env (env ag)
          column-scope-id (id (scope-element ag))
          {table-id :id table-scope-id :scope-id table-schema :schema :as table}
          (when (qualified? identifiers)
            (find-decl env (first identifiers)))
          _ (when schema
              (when (or (not= table-schema schema)
                        (and schema
                             (not=
                              (:table-or-query-name table)
                              (:correlation-name table))))
                (throw (err/illegal-arg
                        :xtdb.sql/analyze-error
                        {::err/message (format "%s is an invalid reference to %s, schema name does not match"
                                               (str/join "." (cons schema identifiers))
                                               (:correlation-name table))}))))
          outer-reference? (and table (< ^long table-scope-id ^long column-scope-id))
          group-env (group-env ag)
          column-reference-type (reduce
                                 (fn [acc {:keys [grouping-columns
                                                  group-column-reference-type]}]
                                   (if (contains? (set grouping-columns) identifiers)
                                     (reduced group-column-reference-type)
                                     acc))
                                 (get (local-env group-env) :column-reference-type :ordinary)
                                 group-env)
          column-reference-type (cond

                                  (#{"system_time"} (second identifiers))
                                  :system-time-period-reference

                                  (#{"valid_time"} (second identifiers))
                                  :application-time-period-reference

                                  outer-reference?
                                  (case column-reference-type
                                    :ordinary :outer
                                    :group-invariant :outer-group-invariant
                                    :within-group-varying :invalid-outer-within-group-varying
                                    :invalid-group-invariant :invalid-outer-group-invariant)

                                  (not (qualified? identifiers))
                                  :unqualified

                                  :else
                                  column-reference-type)]
      (with-meta
        (cond-> {:identifiers identifiers
                 :type column-reference-type
                 :scope-id column-scope-id}
          table (assoc :table-id table-id :table-scope-id table-scope-id))
        (cond-> {:ref ag}
          table (assoc :table table))))))

;; Errors

(defn- local-names [[s :as _env] k]
  (->> (mapcat val s)
       (map k)))

(defn- check-duplicates [label ag xs]
  (vec (for [[x ^long freq] (frequencies xs)
             :when (> freq 1)]
         (format (str label " duplicated: %s %s")
                 x (->line-info-str ag)))))

(defn- check-unsigned-integer [label ag]
  (when-not (r/zmatch ag
              [:signed_numeric_literal
               [:exact_numeric_literal lexeme]]
              ;;=>
              (not (str/includes? lexeme "."))

              [:host_parameter_name _]
              ;;=>
              true

              #"^ROWS?$"
              ;;=>
              true)
    [(format (str label " must be an integer: %s %s")
             (->src-str ag) (->line-info-str ag))]))

(defn- check-aggregate-or-subquery [label ag]
  (r/collect-stop
   (fn [inner-ag]
     (r/zcase
       inner-ag
       :general_set_function
       (r/zmatch
         inner-ag
         [:general_set_function [:computational_operation sf] [:set_quantifier sq] _]
         (when (contains? #{"STDDEV_POP" "STDDEV_SAMP" "VAR_POP" "VAR_SAMP"} sf)
           [(format "%s does not support set quanitifiers (%s): %s %s"
                    sf sq (->src-str ag) (->line-info-str ag))]))

       :aggregate_function
       [(format (str label " cannot contain aggregate functions: %s %s")
                (->src-str ag) (->line-info-str ag))]

       :query_expression
       [(format (str label " cannot contain nested queries: %s %s")
                (->src-str ag) (->line-info-str ag))]
       nil))
   ag))

(defn- check-set-operator [ag]
  (when-let [set-op (set-operator ag)]
    (let [candidates (projected-columns ag)
          degrees (mapv count candidates)]
      (when-not (apply = degrees)
        [(format "%s requires tables to have same degree: %s"
                 set-op (->line-info-str ag))]))))

(defn- check-values [ag]
  (let [candidates (projected-columns ag)
        degrees (mapv count candidates)]
    (when-not (apply = degrees)
      [(format "VALUES requires rows to have same degree: %s"
               (->line-info-str ag))])))

(defn- check-derived-columns [ag]
  (let [{:keys [derived-columns] :as table} (table ag)]
    (when derived-columns
      (when-let [candidates (some->> (:subquery-ref (meta table))
                                     (projected-columns))]
        (let [degrees (mapv count candidates)]
          (when-not (apply = (count derived-columns) degrees)
            [(format "Derived columns has to have same degree as table: %s"
                     (->line-info-str ag))]))))))

(defn- check-period-specification-count [ag]
  (cond-> []
    (< 1 (r/reduce-children + #(when (r/ctor? :query_system_time_period_specification %) 1) ag))
    (conj (format "Each table may only contain a single system time period specification: %s"
                  (->line-info-str ag)))

    (< 1 (r/reduce-children + #(when (r/ctor? :query_valid_time_period_specification %) 1) ag))
    (conj (format "Each table may only contain a single valid time period specification: %s"
                  (->line-info-str ag)))))


(defn- check-column-reference [ag]
  (let [{:keys [identifiers table-id type] :as column-reference} (column-reference ag)]
    (case type
      (:ordinary
       :group-invariant
       :within-group-varying
       :outer
       :outer-group-invariant)
      (if-not table-id
        [(format "Table not in scope: %s %s"
                 (first identifiers) (->line-info-str ag))]
        (let [projection (first (projected-columns (:ref (meta (:table (meta column-reference))))))]
          (cond
            (->> (map :qualified-column projection)
                 (not-any? #{identifiers}))
            [(format "Column not in scope: %s %s"
                     (str/join "." identifiers) (->line-info-str ag))]

            (< 1 (->> (map :identifier projection)
                      (filter #{(last identifiers)})
                      (count)))
            [(format "Column name ambiguous: %s %s"
                     (str/join "." identifiers) (->line-info-str ag))]

            :else
            [])))

      (:application-time-period-reference :system-time-period-reference)
      (if-not table-id
        [(format "Table not in scope: %s %s"
                 (first identifiers) (->line-info-str ag))]

        (let [projection (first (projected-columns (:ref (meta (:table (meta column-reference))))))]
          (cond
            (not-every? (set (map :qualified-column projection))
                          (map :identifiers (expand-underlying-column-references column-reference)))
            [(format "Period not in scope: %s %s"
                     (str/join "." identifiers) (->line-info-str ag))]

            (not (r/ctor? :period_predicand (r/parent ag)))
            [(format "References to periods may only appear within period predicates: %s %s"
                     (str/join "." identifiers) (->line-info-str ag))]
            :else
            [])))

      :unqualified
      (if (order-by-index (:ref (meta column-reference)))
        [] ;;references an unqualified derived column in the select list
        ;; perhaps the below error should mention unqualifed cols are valid within
        ;; order-by if referring to derived col from select list
        [(format "XTDB requires fully-qualified columns: %s %s"
                 (->src-str ag) (->line-info-str ag))])

      :invalid-group-invariant
      [(format "Column reference is not a grouping column: %s %s"
               (->src-str ag) (->line-info-str ag))]

      :invalid-outer-group-invariant
      [(format "Outer column reference is not an outer grouping column: %s %s"
               (->src-str ag) (->line-info-str ag))]

      :invalid-outer-within-group-varying
      [(format "Within group varying column reference is an outer column: %s %s"
               (->src-str ag) (->line-info-str ag))])))

(defn- check-where-clause [ag]
  (r/collect-stop
   (fn [ag]
     (r/zcase ag
       :aggregate_function
       [(format "WHERE clause cannot contain aggregate functions: %s %s"
                (->src-str ag) (->line-info-str ag))]

       :subquery
       []

       nil))
   ag))

(defn subquery-type [ag]
  (r/zcase ag
    (:query_expression
     :in_value_list)
    {:type :scalar_subquery :single? true}

    :array_value_constructor_by_query
    {:type :table_subquery :single? true}

    (:with_list_element
     :table_primary
     :in_predicate_value
     :quantified_comparison_predicate_part_2
     :exists_predicate)
    {:type :table_subquery}

    (:table_value_constructor
     :row_value_constructor
     :explicit_row_value_constructor)
    {:type :row_subquery}

    ::r/inherit))

(defn- check-subquery [ag]
  (let [{:keys [single?]} (subquery-type (r/parent ag))]
    (when (and single? (not= 1 (count (first (projected-columns ag)))))
      [(format "Subquery does not select single column: %s %s"
               (->src-str ag) (->line-info-str ag))])))

(defn- check-from-subquery [ag]
  (r/zmatch ag
            [:from_subquery ^:z cl _qe]
            (let [selected-columns (first (projected-columns (r/$ ag -1)))
                  inserted-columns (identifiers cl)]
              (cond-> []
                (not= (count inserted-columns) (count selected-columns))
                (conj (format "INSERT requires query to have same degree as column list: %s"
                              (->line-info-str ag)))

                (not (some #{"xt$id"} inserted-columns))
                (conj (format "INSERT does not contain mandatory xt$id column: %s"
                              (->line-info-str ag)))))

            [:from_subquery _qe]
            (let [selected-columns (first (projected-columns (r/$ ag -1)))]
              (when-not (some #{"xt$id"} (map :identifier selected-columns))
                [(format "INSERT does not contain mandatory xt$id column: %s"
                         (->line-info-str ag))]))))

(defn- check-select-list [ag]
  (let [projections (projected-columns ag)]

    (when (= [[]] projections)
      [(format "Query does not select any columns: %s"
               (->line-info-str ag))])

    (keep
     #(when (:invalid-ref-to-non-grouping-col %)
        (format "Column %s must appear in GROUP BY clause or be used in aggregate function: %s"
                (str/join "." (:qualified-column %))
                (->line-info-str ag)))
     (first projections))))

(defn- check-asterisked-identifier-chain [ag]
  (let [identifiers (identifiers ag)]
    (when-not (find-local-decl (env ag) (first identifiers))
      [(format "Table not in scope: %s %s"
               (first identifiers) (->line-info-str ag))])))

(defn- check-named-columns-join [ag]
  (let [{:keys [join-columns lhs rhs]} (named-columns-join-env ag)
        join-columns (set join-columns)]
    (->> (for [[label side] [["Left" lhs] ["Right" rhs]]]
           (cond-> []
             (not= join-columns (set (keys side)))
             (conj (format "%s side does not contain all join columns: %s %s"
                           label (->src-str ag) (->line-info-str ag)))

             (not (apply = 1 (map count (vals side))))
             (conj (format "%s side contains ambiguous join columns: %s %s"
                           label (->src-str ag) (->line-info-str ag)))))
         (reduce into))))

(defn- check-dml-non-determinism [ag]
  (r/zcase ag
    :arrow_table
    [(format "Non-deterministic ARROW_TABLE is not allowed in DML statements: %s"
             (->line-info-str ag))]

    []))

(defn- dml-statement? [ag]
  (and (= :directly_executable_statement (r/ctor ag))
       (contains? #{:insert_statement :delete_statement__searched :update_statement__searched :erase_statement__searched}
                  (r/ctor (r/$ ag -1)))))

(defn check-period-specification [ag]
  (if-let [errors (not-empty (r/collect
                               (fn [ag]
                                 (when (r/ctor? :column_reference ag)
                                   [(format "Columns are not valid within period specifications: %s %s"
                                            (->src-str ag)
                                            (->line-info-str ag))]))
                               ag))]
    errors
    []))

(defn- check-set-clause [ag]
  (r/zmatch ag
    [:set_clause [:update_target ^:z id] _ _]
    (concat (when (contains? #{"xt$valid_from" "xt$valid_to"} (identifier id))
              [(format "Updating app-time columns outside of `FOR PERIOD OF` is not supported: %s %s"
                       (->src-str ag)
                       (->line-info-str ag))])

            (when (contains? #{"xt$system_from" "xt$system_to"} (identifier id))
              [(format "Updating system-time columns is not supported: %s %s"
                       (->src-str ag)
                       (->line-info-str ag))]))))

(defn errs [ag]
  (let [dml? (dml-statement? ag)]
    (r/collect
     (fn [ag]
       (r/zcase ag
         :table_reference_list
         (check-duplicates "Table variable" ag
                           (local-names (dclo ag) :correlation-name))

         :with_clause (if (= (r/lexeme ag 2) "RECURSIVE")
                        [(format "RECURSIVE WITH clauses are not supported: %s %s"
                                 (->src-str ag)
                                 (->line-info-str ag))]
                        [])
         :with_list
         (check-duplicates "CTE query name" ag
                           (local-names (cteo ag) :query-name))

         (:query_expression_body
          :query_term)
         (check-set-operator ag)

         :select_list
         (check-select-list ag)

         :asterisked_identifier_chain
         (check-asterisked-identifier-chain ag)

         :table_value_constructor
         (check-values ag)

         :table_primary
         (concat
           (check-derived-columns ag)
           (check-period-specification-count ag))

         :column_name_list
         (check-duplicates "Column name" ag (identifiers ag))

         :column_reference
         (check-column-reference ag)

         (:general_set_function
          :array_aggregate_function)
         (check-aggregate-or-subquery "Aggregate functions" ag)

         :sort_specification
         (check-aggregate-or-subquery "Sort specifications" ag)

         :where_clause
         (check-where-clause ag)

         :fetch_first_clause
         (if (= "LIMIT" (r/lexeme ag 1))
           (check-unsigned-integer "Fetch first row count" (r/$ ag 2))
           (check-unsigned-integer "Fetch first row count" (r/$ ag 3)))

         :result_offset_clause
         (check-unsigned-integer "Offset row count" (r/$ ag 2))

         :subquery
         (check-subquery ag)

         :named_columns_join
         (check-named-columns-join ag)

         :from_subquery
         (check-from-subquery ag)

         (:query_system_time_period_specification
          :query_valid_time_period_specification)
         (check-period-specification ag)

         :set_clause
         (check-set-clause ag)

         (if dml?
           (check-dml-non-determinism ag)
           [])))
     ag)))

;; Scopes

(defn scope-element [ag]
  (r/zcase ag
    (:query_expression
     :query_specification
     :delete_statement__searched
     :update_statement__searched
     :erase_statement__searched)
    ag

    ::r/inherit))

(defn scope
  "Currently only used in tests to check ASTs are valid,
  might be worth bringing is as part of getting CTEs fully working"
  [ag]
  (r/zcase ag
    (:query_expression
     :query_specification)
    (let [scope-id (id ag)
          parent-id (some-> (scope-element (r/parent ag)) (id))
          all-columns (all-column-references ag)
          table-id->all-columns (->> (group-by :table-id all-columns)
                                     (into (sorted-map)))
          ;; NOTE: assumes that tables declared in
          ;; outer scopes have lower ids than the
          ;; current scope.
          dependent-columns (->> (subseq (dissoc table-id->all-columns nil) < scope-id)
                                 (mapcat val)
                                 (set))
          projected-columns (->> (projected-columns ag)
                                 (first)
                                 (mapv #(dissoc % :normal-form)))
          scope (with-meta
                  (cond-> {:id scope-id
                           :dependent-columns dependent-columns
                           :projected-columns projected-columns}
                    parent-id (assoc :parent-id parent-id))
                  {:ref ag})]
      (r/zcase ag
        :query_expression
        (let [local-ctes (local-env-singleton-values (cteo ag))
              order-by-indexes (order-by-indexes ag)]
          (cond-> (assoc scope
                         :type :query-expression
                         :ctes local-ctes)
            (not-empty order-by-indexes) (assoc :order-by-indexes order-by-indexes)))

        :query_specification
        (let [local-tables (local-tables ag)
              local-columns (set (get (group-by :scope-id all-columns) scope-id))
              local-tables (->> (for [{:keys [id correlation-name] :as table} local-tables
                                      :let [used-columns (->> (get table-id->all-columns id)
                                                              (map :identifiers)
                                                              (set))]]
                                  [correlation-name (assoc table :used-columns used-columns)])
                                (into {}))
              grouping-columns (grouping-columns ag)]
          (cond-> (assoc scope
                         :tables local-tables
                         :columns local-columns
                         :type :query-specification)
            grouping-columns (assoc :grouping-columns grouping-columns)))))

    ::r/inherit))

(doseq [[_ v] (ns-publics *ns*)
        :let [{:keys [arglists private]} (meta v)]
        :when (and (= arglists '([ag]))
                   (not private))]
  (alter-var-root v r/zmemoize))

(defn analyze-query [ast]
  (let [ag (r/vector-zip ast)]
    {:errs (errs ag), :ag ag}))

(defn or-throw [{:keys [errs ag]}]
  (if (seq errs)
    (throw (err/illegal-arg :xtdb.sql/analyze-error
                            {::err/message (str "Invalid SQL query:\n  - "
                                                (str/join "\n  - " errs))
                             :errs errs}))
    ag))
