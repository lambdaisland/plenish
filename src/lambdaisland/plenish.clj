(ns lambdaisland.plenish
  "Transfer datomic data into a relational target database, transaction per
  transaction."
  (:require [charred.api :as charred]
            [clojure.string :as str]
            [datomic.api :as d]
            [honey.sql :as honey]
            [honey.sql.helpers :as hh]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(set! *warn-on-reflection* true)

;; Basic helpers

(defn has-attr?
  "Does the entity `eid` have the attribute `attr` in the database `db`.

  Uses direct index access so we can cheaply check if a give entity should be
  added to a given table, based on a membership attribute."
  [db eid attr]
  (-> db
      ^Iterable (d/datoms :eavt eid attr)
      .iterator
      .hasNext))

;;;;;;;;;;;;;;;;
;; Datom helpers
(def -e "Get the entity id of a datom" (memfn ^datomic.Datom e))
(def -a "Get the attribute of a datom" (memfn ^datomic.Datom a))
(def -v "Get the value of a datom" (memfn ^datomic.Datom v))
(def -t "Get the transaction number of a datom" (memfn ^datomic.Datom tx))
(def -added? "Has this datom been added or retracted?" (memfn ^datomic.Datom added))

;;;;;;;;;;;;;;;;;;;
;; Context helpers

;; All through the process we pass around a `ctx` map, which encapsulates the
;; state of the export/import process.

;; - `:idents` map from eid (entity id) to entity/value map
;; - `:entids` map from ident to eid (reverse lookup)
;; - `:tables` map from membership attribute to table config, as per Datomic
;;   Analytics metaschema We also additionally store a `:columns` map for each
;;   table, to track which columns already exist in the target db.
;; - `:db-types` mapping from datomic type to target DB type
;; - `:ops` vector of "operations" that need to be propagated, `[:upsert ...]`, `[:delete ...]`, etc.

(defn ctx-ident
  "Find an ident (keyword) by eid"
  [ctx eid]
  (get-in ctx [:idents eid :db/ident]))

(defn ctx-entid
  "Find the numeric eid for a given ident (keyword)"
  [ctx ident]
  (get-in ctx [:entids ident]))

(defn ctx-valueType
  "Find the valueType (keyword, e.g. `:db.type/string`) for a given attribute (as eid)"
  [ctx attr-id]
  (ctx-ident ctx (get-in ctx [:idents attr-id :db/valueType])))

(defn ctx-cardinality
  "Find the cardinality (`:db.cardinality/one` or `:db.cardinality/many`) for a
  given attribute (as eid)"
  [ctx attr-id]
  (ctx-ident ctx (get-in ctx [:idents attr-id :db/cardinality])))

(defn ctx-card-many?
  "Returns true if the given attribute (given as eid) has a cardinality of `:db.cardinality/many`"
  [ctx attr-id]
  (= :db.cardinality/many (ctx-cardinality ctx attr-id)))

(defn dash->underscore
  "Replace dashes with underscores in string s"
  [s]
  (str/replace s #"-" "_"))

;; A note on naming, PostgreSQL (our primary target) does not have the same
;; limitations on names that Presto has. We can use e.g. dashes just fine,
;; assuming names are properly quoted. We've still opted to use underscores by
;; default, to make ad-hoc querying easier (quoting will often not be
;; necessary), and to have largely the same table structure as datomic
;; analytics, to make querying easier.
;;
;; That said we don't munge anything else (like a trailing `?` and `!`), and do
;; rely on PostgreSQL quoting to handle these.

(defn table-name
  "Find a table name for a given table based on its membership attribute, either
  as configured in the metaschema (`:name`), or derived from the namespace of
  the membership attribute."
  [ctx mem-attr]
  (get-in ctx
          [:tables mem-attr :name]
          (dash->underscore (namespace mem-attr))))

(defn join-table-name
  "Find a table name for a join table, i.e. a table that is created because of a
  cardinality/many attribute, given the membership attribute of the base table,
  and the cardinality/many attribute. Either returns a name as configured in the
  metaschema under `:rename-many-table`, or derives a name as
  `namespace_mem_attr_x_name_val_attr`."
  [ctx mem-attr val-attr]
  (get-in ctx
          [:tables mem-attr :rename-many-table val-attr]
          (dash->underscore (str (table-name ctx mem-attr) "_x_" (name val-attr)))))

(defn column-name
  "Find a name for a column based on the table membership attribute, and the
  attribute whose values are to be stored in the column."
  [ctx mem-attr col-attr]
  (get-in ctx
          [:tables mem-attr :rename col-attr]
          (dash->underscore (name col-attr))))

(defn attr-db-type
  "Get the target db type for the given attribute."
  [ctx attr-id]
  (get-in ctx [:db-types (ctx-valueType ctx attr-id)]))

;; Transaction processing logic

(defn track-idents
  "Keep `:entids` and `:idents` up to date based on tx-data. This allows us to
  incrementally track schema changes, so we always have the right metadata at
  hand."
  ;; This has a notable shortcoming in that we currently don't treat
  ;; `cardinality/many` in any special way. In the initial `pull-idents` these will
  ;; come through as collections, but any later additions will replace these with
  ;; single values.
  ;;
  ;; However, the attributes we care about so far (`:db/ident`,
  ;; `:db/cardinality`, `:db/valueType`) are all of cardinality/one, so for the
  ;; moment this is not a great issue.
  [ctx tx-data]
  (let [db-ident  (get-in ctx [:entids :db/ident])
        tx-idents (filter #(= db-ident (-a %)) tx-data)
        tx-rest   (remove #(= db-ident (-a %)) tx-data)]
    (as-> ctx ctx
      ;; Handle `[_ :db/ident _]` datoms first. We can't rely on any ordering of
      ;; datoms within a transaction. If a transaction adds both `:db/ident` and
      ;; other attributes, then we need to process the `:db/ident` datom first,
      ;; so that when we process the rest of the datoms we can see that this eid
      ;; is an ident.
      (reduce (fn [ctx datom]
                (let [e     (-e datom)
                      a     (-a datom)
                      ident (-v datom)]
                  (if (-added? datom)
                    (-> ctx
                        (update :entids assoc ident e)
                        (update-in [:idents e] assoc
                                   :db/id e
                                   :db/ident ident))
                    (-> ctx
                        (update :entids dissoc ident)
                        (update :idents dissoc e)))))
              ctx
              tx-idents)
      ;; Handle non `[_ :db/ident _]` datoms
      (reduce (fn [ctx datom]
                (let [e (-e datom)
                      a (-a datom)]
                  (if (get-in ctx [:idents e])
                    (-> ctx
                        (update-in
                         [:idents e]
                         (fn [m]
                           (let [attr (ctx-ident ctx (-a datom))]
                             (if (-added? datom)
                               (assoc m attr (-v datom))
                               (dissoc m attr))))))
                    ctx)))
              ctx
              tx-rest))))

(defn encode-value
  "Do some pre-processing on a value based on the datomic value type, so that
  HoneySQL/JDBC is happy with it. Somewhat naive and postgres specific right
  now."
  [ctx type value]
  (case type
    :db.type/ref (if (keyword? value)
                   (ctx-entid ctx value)
                   value)
    :db.type/tuple [:raw (str \' (str/replace (str (charred/write-json-str value)) "'" "''") \' "::jsonb")]
    :db.type/keyword (str (when (qualified-ident? value)
                            (str (namespace value) "/"))
                          (name value))
    :db.type/instant [:raw (format "to_timestamp(%.3f)" (double (/ (.getTime ^java.util.Date value) 1000)))]
    value))

(defn card-one-entity-ops
  "Add operations `:ops` to the context for all the `cardinality/one` datoms in a
  single transaction and a single table, identified by its memory attribute."
  [{:keys [tables] :as ctx} mem-attr eid datoms]
  (let [missing-cols (sequence
                      (comp
                       (remove (fn [d]
                                 ;; If there's already a `:columns` entry in the
                                 ;; context, then this column already exists in
                                 ;; the target DB, if not it needs to be
                                 ;; created. This is heuristic, we do a
                                 ;; conditional alter table, so no biggie if it
                                 ;; already exists.
                                 (get-in ctx [:tables mem-attr :columns (ctx-ident ctx (-a d))])))
                       (map -a)
                       (map (fn [attr-id]
                              (let [attr (ctx-ident ctx attr-id)]
                                [attr
                                 {:name (column-name ctx mem-attr attr)
                                  :type (attr-db-type ctx attr-id)}]))))
                      datoms)
        retracted?   (some (fn [d]
                             ;; Datom with membership attribute was retracted,
                             ;; remove from table
                             (and (not (-added? d))
                                  (= mem-attr (ctx-ident ctx (-a d)))))
                           datoms)]
    (cond-> ctx
      ;; Evolve the schema
      (seq missing-cols)
      (-> (update :ops
                  (fnil conj [])
                  [:ensure-columns
                   {:table   (table-name ctx mem-attr)
                    :columns (into {} missing-cols)}])
          (update-in [:tables mem-attr :columns] (fnil into {}) missing-cols))
      ;; Delete/upsert values
      :->
      (update :ops (fnil conj [])
              (if retracted?
                [:delete
                 {:table  (table-name ctx mem-attr)
                  :values {:db/id eid}}]
                [:upsert
                 {:table  (table-name ctx mem-attr)
                  :values (into (cond-> {"db__id" eid}
                                  ;; Bit of manual fudgery to also get the "t"
                                  ;; value of each transaction into
                                  ;; our "transactions" table.
                                  (= :db/txInstant mem-attr)
                                  (assoc "t" (d/tx->t (-t (first datoms)))))
                                (map (juxt #(column-name ctx mem-attr (ctx-ident ctx (-a %)))
                                           #(when (-added? %)
                                              (encode-value ctx
                                                            (ctx-valueType ctx (-a %))
                                                            (-v %)))))
                                datoms)}])))))

(defn card-many-entity-ops
  "Add operations `:ops` to the context for all the `cardinality/many` datoms in a
  single transaction and a single table, identified by its memory attribute.
  Each `:db.cardinality/many` attribute results in a separate two-column join
  table."
  [{:keys [tables] :as ctx} mem-attr eid datoms]
  (let [missing-joins (sequence
                       (comp
                        (remove #(get-in ctx [:tables mem-attr :join-tables (ctx-ident ctx (-a %))]))
                        (map -a)
                        (distinct)
                        (map (fn [attr-id]
                               (let [attr (ctx-ident ctx attr-id)]
                                 [attr
                                  {:name (column-name ctx mem-attr attr)
                                   :type (attr-db-type ctx attr-id)}]))))
                       datoms)]
    (cond-> ctx
      (seq missing-joins)
      (-> (update :ops
                  (fnil into [])
                  (for [[val-attr join-opts] missing-joins]
                    [:ensure-join
                     {:table (join-table-name ctx mem-attr val-attr)
                      :fk-table (table-name ctx mem-attr)
                      :val-attr val-attr
                      :val-col (column-name ctx mem-attr val-attr)
                      :val-type (:type join-opts)} ]))
          (update-in [:tables mem-attr :join-tables] (fnil into {}) missing-joins))
      :->
      (update :ops
              (fnil into [])
              (for [d datoms]
                (let [attr-id (-a d)
                      attr (ctx-ident ctx attr-id)
                      value (-v d)]
                  [(if (-added? d) :upsert :delete)
                   {:table (join-table-name ctx mem-attr attr)
                    :values {"db__id" eid
                             (column-name ctx mem-attr attr)
                             (encode-value ctx (ctx-valueType ctx attr-id) value)}}]))))))

(def ignore-idents #{:db/ensure :db/fn})

(defn process-entity
  "Process the datoms within a transaction for a single entity. This checks all
  tables to see if the entity contains the membership attribute, if so
  operations get added under `:ops` to evolve the schema and insert the data."
  [{:keys [tables] :as ctx} prev-db db eid datoms]
  (reduce
   (fn [ctx [mem-attr table-opts]]
     (if (has-attr? db eid mem-attr)
       ;; Handle cardinality/one separate from cardinality/many
       (let [datoms           (if (not (has-attr? prev-db eid mem-attr))
                                ;; If after the previous transaction the
                                ;; membership attribute wasn't there yet, then
                                ;; it's added in this tx. In that case pull in
                                ;; all pre-existing datoms for the entities,
                                ;; they need to make across as well.
                                (concat datoms (d/datoms prev-db :eavt eid))
                                datoms)
             datoms           (remove (fn [d] (contains? ignore-idents (ctx-ident ctx (-a d)))) datoms)
             card-one-datoms  (remove (fn [d] (ctx-card-many? ctx (-a d))) datoms)
             card-many-datoms (filter (fn [d] (ctx-card-many? ctx (-a d))) datoms)]
         (-> ctx
             (card-one-entity-ops mem-attr eid card-one-datoms)
             (card-many-entity-ops mem-attr eid card-many-datoms)))
       ctx))
   ctx
   tables))

(defn process-tx
  "Handle a single datomic transaction, this will update the context, appending to
  the `:ops` the operations needed to propagate the update, while also keeping
  the rest of the context (`ctx`) up to date, in particular by tracking any
  schema changes, or other changes involving `:db/ident`."
  [ctx conn {:keys [t data]}]
  (let [ctx (track-idents ctx data)
        prev-db (d/as-of (d/db conn) (dec t))
        db (d/as-of (d/db conn) t)
        entities (group-by -e data)]
    (reduce (fn [ctx [eid datoms]]
              (process-entity ctx prev-db db eid datoms))
            ctx
            entities)))

;; Up to here we've only dealt with extracting information from datomic
;; transactions, and turning them into
;; abstract "ops" (:ensure-columns, :upsert, :delete, etc). So this is all
;; target-database agnostic, what's left is to turn this into SQL and sending it
;; to the target.

;; Converting ops to SQL

(def pg-type
  {:db.type/ref :bigint
   :db.type/keyword :text
   :db.type/long :bigint
   :db.type/string :text
   :db.type/boolean :boolean
   :db.type/uuid :uuid
   :db.type/instant :timestamp ;; no time zone information in java.util.Date
   :db.type/double [:float 53]
   ;;   :db.type/fn
   :db.type/float [:float 24]
   :db.type/bytes :bytea
   :db.type/uri :text
   :db.type/bigint :numeric
   :db.type/bigdec :numeric
   :db.type/tuple :jsonb})

(defmulti op->sql
  "Convert a single operation (two element vector), into a sequence of HoneySQL
  maps."
  first)

(defmethod op->sql :ensure-columns [[_ {:keys [table columns]}]]
  (into
   [{:create-table [table :if-not-exists],
     :with-columns [[:db__id [:raw "bigint"] [:primary-key]]]}]
   (map (fn [[_ {:keys [name type]}]]
          {:alter-table [table]
           :add-column [(keyword name)
                        (if (keyword? type)
                          ;; Annoyingly this is needed because we use `:quote true`, and honey tries to quote the type
                          [:raw (clojure.core/name type)]
                          type)
                        :if-not-exists]}))
   columns))

(defmethod op->sql :upsert [[_ {:keys [table values]}]]
  [{:insert-into   [(keyword table)]
    :values        [values]
    :on-conflict   [:db__id]
    :do-update-set (keys (dissoc values "db__id"))}])

(defmethod op->sql :ensure-join [[_ {:keys [table val-col val-type]}]]
  [{:create-table [table :if-not-exists],
    :with-columns [[:db__id [:raw "bigint"] [:primary-key]]
                   [(keyword val-col) (if (keyword? val-type)
                                        [:raw (name val-type)]
                                        type)]]}])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Top level process

(defn- pull-idents
  "Do a full `(pull [*])` of all database entities that have a `:db/ident`, used
  to bootstrap the context, we track all idents and their
  metadata (`:db/valueType`, `:db/cardinality` etc in memory on our inside
  inside a `ctx` map)."
  [db]
  (d/q
   '[:find [(pull ?e [*]) ...]
     :where [?e :db/ident]]
   db))

(defn initial-ctx
  "Create the context map that gets passed around all through the process,
  contains both caches for quick lookup of datomic schema information,
  configuration regarding tables and target db, and eventually `:ops` that need
  to be processed."
  [conn metaschema]
  ;; Bootstrap, make sure we have info about idents that datomic creates itself
  ;; at db creation time. d/as-of t=999 is basically an empty database with only
  ;; metaschema attributes (:db/txInstant etc), since the first "real"
  ;; transaction is given t=1000. Interesting to note that Datomic seems to
  ;; bootstrap in pieces: t=0 most basic idents, t=57 add double, t=63 add
  ;; docstrings, ...
  (let [idents (pull-idents (d/as-of (d/db conn) 999))]
    {:entids (into {} (map (juxt :db/ident :db/id)) idents)
     :idents (into {} (map (juxt :db/id identity)) idents)
     :tables (update (:tables metaschema)
                     :db/txInstant
                     assoc :name "transactions")
     :db-types pg-type
     :ops [[:ensure-columns
            {:table   "transactions"
             :columns {:t {:name "t"
                           :type :bigint}}}]]}))

(defn import-tx-range
  "Import a range of transactions (e.g. from [[d/tx-range]]) into the target
  database. Takes a `ctx` as per [[initial-ctx]], a datomic connection `conn`,
  and a JDBC datasource `ds`"
  [ctx conn ds tx-range]
  (loop [ctx ctx
         [tx & txs] tx-range]
    (if tx
      (let [ctx (process-tx ctx conn tx)
            queries (eduction
                     (comp
                      (mapcat op->sql)
                      (map #(honey/format % {:quoted true})))
                     (:ops ctx))]
        #_(run! prn (:ops ctx))
        (run! #(do #_(clojure.pprint/pprint %)
                   (jdbc/execute! ds %)) queries)
        (recur (dissoc ctx :ops) txs))
      ctx)))
