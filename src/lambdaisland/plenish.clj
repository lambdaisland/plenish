(ns lambdaisland.plenish
  "Transfer datomic data into a relational target database, transaction per
  transaction."
  (:require
   [charred.api :as charred]
   [clojure.string :as str]
   [datomic.api :as d]
   [honey.sql :as honey]
   [honey.sql.helpers :as hh]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [lambdaisland.plenish.protocols :as proto]))

(set! *warn-on-reflection* true)

(def ^:dynamic *debug* (= (System/getenv "PLENISH_DEBUG") "true"))
(defn dbg [& args] (when *debug* (prn args)))

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

;; The functions below are the heart of the process

;; process-tx                       -  process all datoms in a single transaction
;; \___ process-entity              -  process datoms within a transaction with the same entity id
;;      \____ card-one-entity-ops   -  process datoms for all cardinality/one attributes of a single entity
;;      \____ card-many-entity-ops  -  process datoms for all cardinality/many attributes of a single entity

(defn card-one-entity-ops
  "Add operations `:ops` to the context for all the `cardinality/one` datoms in a
  single transaction and a single entity/table, identified by its memory
  attribute."
  [{:keys [tables db-adapter] :as ctx} mem-attr eid datoms]
  {:pre [(every? #(= eid (-e %)) datoms)]}
  (let [;; An update of an attribute will manifest as two datoms in the same
        ;; transaction, one with added=true, and one with added=false. In this
        ;; case we can ignore the added=false datom.
        datoms (remove (fn [d]
                         (and (not (-added? d))
                              (some #(and (= (-a d) (-a %))
                                          (-added? %)) datoms)))
                       datoms)
        ;; Figure out which columns don't exist yet in the target database. This
        ;; may find columns that actually do already exist, depending on the
        ;; state of the context. This is fine, we'll process these schema
        ;; changes in an idempotent way, we just want to prevent us from having
        ;; to attempt schema changes for every single transaction.
        missing-cols (sequence
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
        ;; Do we need to delete the row corresponding with this entity.
        retracted? (and (some (fn [d]
                                ;; Datom with membership attribute was retracted,
                                ;; remove from table
                                (and (not (-added? d))
                                     (= mem-attr (ctx-ident ctx (-a d)))))
                              datoms)
                        (not (some (fn [d]
                                     ;; Unless the same transaction
                                     ;; immediately adds a new datom with the
                                     ;; membership attribute
                                     (and (-added? d)
                                          (= mem-attr (ctx-ident ctx (-a d)))))
                                   datoms)))]
    ;;(clojure.pprint/pprint ['card-one-entity-ops mem-attr eid datoms retracted?])
    (cond-> ctx
      ;; Evolve the schema
      (seq missing-cols)
      (-> (update :ops
                  (fnil conj [])
                  [:ensure-columns
                   {:table   (table-name ctx mem-attr)
                    :columns (into {} missing-cols)}])
          (update-in [:tables mem-attr :columns] (fnil into {}) missing-cols))
      ;; Delete/insert values
      :->
      (update :ops (fnil conj [])
              (if retracted?
                [:delete
                 {:table  (table-name ctx mem-attr)
                  :values {"db__id" eid}}]
                (let [table (table-name ctx mem-attr)]
                  [(if (= "transactions" table)
                     :insert
                     :upsert)
                   {:table table
                    :by #{"db__id"}
                    :values (into (cond-> {"db__id" eid}
                                    ;; Bit of manual fudgery to also get the "t"
                                    ;; value of each transaction into
                                    ;; our "transactions" table.
                                    (= :db/txInstant mem-attr)
                                    (assoc "t" (d/tx->t (-t (first datoms)))))
                                  (map (juxt #(column-name ctx mem-attr (ctx-ident ctx (-a %)))
                                             #(when (-added? %)
                                                (proto/encode-value db-adapter
                                                                    ctx
                                                                    (ctx-valueType ctx (-a %))
                                                                    (-v %)))))
                                  datoms)}]))))))

(defn card-many-entity-ops
  "Add operations `:ops` to the context for all the `cardinality/many` datoms in a
  single transaction and a single table, identified by its memory attribute.
  Each `:db.cardinality/many` attribute results in a separate two-column join
  table."
  [{:keys [tables db-adapter] :as ctx} mem-attr eid datoms]
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
                      :val-type (:type join-opts)}]))
          (update-in [:tables mem-attr :join-tables] (fnil into {}) missing-joins))
      :->
      (update :ops
              (fnil into [])
              (for [d datoms]
                (let [attr-id (-a d)
                      attr (ctx-ident ctx attr-id)
                      value (-v d)
                      sql-table (join-table-name ctx mem-attr attr)
                      sql-col (column-name ctx mem-attr attr)
                      sql-val (proto/encode-value db-adapter ctx (ctx-valueType ctx attr-id) value)]
                  (if (-added? d)
                    [:upsert
                     {:table sql-table
                      :by #{"db__id" sql-col}
                      :values {"db__id" eid
                               sql-col sql-val}}]
                    [:delete
                     {:table sql-table
                      :values {"db__id" eid
                               sql-col sql-val}}])))))))

(def ignore-idents #{:db/ensure
                     :db/fn
                     :db.install/valueType
                     :db.install/attribute
                     :db.install/function
                     :db.entity/attrs
                     :db.entity/preds
                     :db.attr/preds})

(defn process-entity
  "Process the datoms within a transaction for a single entity. This checks all
  tables to see if the entity contains the membership attribute, if so
  operations get added under `:ops` to evolve the schema and insert the data."
  [{:keys [tables] :as ctx} prev-db db eid datoms]
  ;;(clojure.pprint/pprint ['process-entity eid datoms])
  (reduce
   (fn [ctx [mem-attr table-opts]]
     (if (or (has-attr? prev-db eid mem-attr)
             (has-attr? db eid mem-attr))
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
         (cond-> ctx
           (seq card-one-datoms)
           (card-one-entity-ops mem-attr eid card-one-datoms)

           (seq card-many-datoms)
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
   [{:create-table [table :if-not-exists]
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

(defmethod op->sql :insert [[_ {:keys [table by values]}]]
  [{:insert-into [(keyword table)]
    :values      [values]}])

(defmethod op->sql :upsert [[_ {:keys [table by values]}]]
  (let [op {:insert-into   [(keyword table)]
            :values        [values]
            :on-conflict   (map keyword by)}
        attrs (apply dissoc values by)]
    [(if (seq attrs)
       (assoc op :do-update-set (keys attrs))
       (assoc op :do-nothing []))]))

(defmethod op->sql :delete [[_ {:keys [table values]}]]
  [{:delete-from (keyword table)
    :where (reduce-kv (fn [clause k v]
                        (conj clause [:= k v]))
                      [:and]
                      (update-keys values keyword))}])

(defmethod op->sql :ensure-join [[_ {:keys [table val-col val-type]}]]
  [{:create-table [table :if-not-exists]
    :with-columns [[:db__id [:raw "bigint"]]
                   [(keyword val-col) (if (keyword? val-type)
                                        [:raw (name val-type)]
                                        val-type)]]}
   ;; cardinality/many attributes are not multi-set, a given triplet can only be
   ;; asserted once, so a given [eid value] for a given attribute has to be
   ;; unique.
   {::create-index {:on table
                    :name (str "unique_attr_" table "_" val-col)
                    :unique? true
                    :if-not-exists? true
                    :columns [:db__id (keyword val-col)]}}])

;; HoneySQL does not support CREATE INDEX. It does support adding indexes
;; through ALTER TABLE, but that doesn't seem to give us a convenient way to
;; sneak in the IF NOT EXISTS. Namespacing this becuase it's a pretty
;; bespoke/specific implementation which we don't want to leak into application
;; code.
(honey/register-clause!
 ::create-index
 (fn [clause {:keys [on name unique? if-not-exists? columns]}]
   [(str "CREATE " (when unique? "UNIQUE ") "INDEX "
         (when if-not-exists? "IF NOT EXISTS ")
         (when name (str (honey/format-entity name) " "))
         "ON " (honey/format-entity on)
         " (" (str/join ", " (map honey/format-entity columns)) ")")])
 :alter-table)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Top level process

(defn- pull-idents
  "Do a full `(pull [*])` of all database entities that have a `:db/ident`, used
  to bootstrap the context, we track all idents and their
  metadata (`:db/valueType`, `:db/cardinality` etc in memory on our inside
  inside a `ctx` map)."
  [db]
  (map
   (fn [ident-attrs]
     (update-vals
      ident-attrs
      (fn [v]
        (if-let [id (when (map? v)
                      (:db/id v))]
          id
          v))))
   (d/q
    '[:find [(pull ?e [*]) ...]
      :where [?e :db/ident]]
    db)))

(defn initial-ctx
  "Create the context map that gets passed around all through the process,
  contains both caches for quick lookup of datomic schema information,
  configuration regarding tables and target db, and eventually `:ops` that need
  to be processed."
  ([conn metaschema db-adapter]
   (initial-ctx conn metaschema db-adapter nil))
  ([conn metaschema db-adapter t]
   ;; Bootstrap, make sure we have info about idents that datomic creates itself
   ;; at db creation time. d/as-of t=999 is basically an empty database with only
   ;; metaschema attributes (:db/txInstant etc), since the first "real"
   ;; transaction is given t=1000. Interesting to note that Datomic seems to
   ;; bootstrap in pieces: t=0 most basic idents, t=57 add double, t=63 add
   ;; docstrings, ...
   (let [idents (pull-idents (d/as-of (d/db conn) (or t 999)))]
     {;; Track datomic schema
      :entids (into {} (map (juxt :db/ident :db/id)) idents)
      :idents (into {} (map (juxt :db/id identity)) idents)
      ;; Configure/track relational schema
      :tables (-> metaschema
                  :tables
                  (update :db/txInstant assoc :name "transactions")
                  (update :db/ident assoc :name "idents"))
      :db-adapter db-adapter
      ;; Mapping from datomic to relational type
      :db-types (proto/db-type db-adapter)
      ;; Create two columns that don't have a attribute as such in datomic, but
      ;; which we still want to track
      :ops [[:ensure-columns
             {:table   "idents"
              :columns {:db/id {:name "db__id"
                                :type :bigint}}}]
            [:ensure-columns
             {:table   "transactions"
              :columns {:t {:name "t"
                            :type :bigint}}}]]})))

(defn import-tx-range
  "Import a range of transactions (e.g. from [[d/tx-range]]) into the target
  database. Takes a `ctx` as per [[initial-ctx]], a datomic connection `conn`,
  and a JDBC datasource `ds`"
  [ctx conn ds tx-range]
  (loop [ctx ctx
         [tx & txs] tx-range
         cnt 1]
    (when (= (mod cnt 100) 0)
      (print ".") (flush))
    (when (= (mod cnt 1000) 0)
      (println (str "\n" (java.time.Instant/now))) (flush))
    (if tx
      (let [ctx (process-tx ctx conn tx)
            queries (eduction
                     (comp
                      (mapcat op->sql)
                      (map #(honey/format % {:quoted true})))
                     (:ops ctx))]
        ;; Each datomic transaction gets committed within a separate JDBC
        ;; transaction, and this includes adding an entry to the "transactions"
        ;; table. This allows us to see exactly which transactions have been
        ;; imported, and to resume work from there.
        (dbg 't '--> (:t tx))
        (jdbc/with-transaction [jdbc-tx ds]
          (run! dbg (:ops ctx))
          (run! #(do (dbg %)
                     (jdbc/execute! jdbc-tx %)) queries))
        (recur (dissoc ctx :ops) txs
               (inc cnt)))
      ctx)))

(defn find-max-t
  "Find the highest value in the transactions table in postgresql. The sync should
  continue from `(inc (find-max-t ds))`"
  [ds]
  (:max
   (update-keys
    (first
     (try
       (jdbc/execute! ds ["SELECT max(t) FROM transactions"])
       (catch Exception e
        ;; If the transactions table doesn't yet exist, return `nil`, so we start
        ;; from the beginning of the log
         nil)))
    (constantly :max))))

(defn sync-to-latest
  "Convenience function that combines the ingredients above for the common case of
  processing all new transactions up to the latest one."
  [datomic-conn pg-conn metaschema db-adapter]
  (let [;; Find the most recent transaction that has been copied, or `nil` if this
        ;; is the first run
        max-t (find-max-t pg-conn)

        ;; Query the current datomic schema. Plenish will track schema changes as
        ;; it processes transcations, but it needs to know what the schema looks
        ;; like so far.
        ctx   (initial-ctx datomic-conn metaschema db-adapter max-t)

        ;; Grab the datomic transactions you want Plenish to process. This grabs
        ;; all transactions that haven't been processed yet.
        txs   (d/tx-range (d/log datomic-conn) (when max-t (inc max-t)) nil)]

    ;; Get to work
    (import-tx-range ctx datomic-conn pg-conn txs)))
