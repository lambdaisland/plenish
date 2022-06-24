(ns repl-sessions.plenish-first-spike
  (:require [datomic.api :as d]
            [honey.sql :as honey]
            [honey.sql.helpers :as hh]
            [lambdaisland.facai :as f]
            [lambdaisland.facai.datomic-peer :as fd]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]
            [charred.api :as charred]))

(d/create-database "datomic:mem://foo")
(def conn (d/connect "datomic:mem://foo"))
(def conn (d/connect "datomic:dev://localhost:4334/example-tenant---import-company-20210901T110746698441277"))
(def ds
  (jdbc/get-datasource
   "jdbc:pgsql://localhost:5432/replica?user=plenish&password=plenish"
   ))

;; docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres
;; docker ps
;; docker exec -it -u postgres <container> psql

CREATE DATABASE replica;
CREATE ROLE plenish WITH LOGIN PASSWORD 'plenish';
GRANT ALL ON DATABASE replica TO plenish;

(f/defactory line-item
  {:line-item/description "Widgets"
   :line-item/quantity 5
   :line-item/price 1.0})

(f/defactory cart
  {:cart/created-at #(java.util.Date.)
   :cart/line-items [line-item line-item]})

(def metaschema
  #_
  {:tables {:line-item/price {:name "line_items"}
            :cart/created-at {:name "cart"}
            #_#_:db/txInstant {:name "transactions"}
            #_#_:db/ident {:name "idents"}}}
  (read-string (slurp "/home/arne/Eleven/runeleven/melvn/presto-etc/datomic/accounting.edn")))

(def schema
  [{:db/ident       :line-item/description,
    :db/valueType   :db.type/string,
    :db/cardinality :db.cardinality/one}
   {:db/ident       :line-item/quantity,
    :db/valueType   :db.type/long,
    :db/cardinality :db.cardinality/one}
   {:db/ident       :line-item/price,
    :db/valueType   :db.type/double,
    :db/cardinality :db.cardinality/one}
   {:db/ident       :cart/created-at,
    :db/valueType   :db.type/instant,
    :db/cardinality :db.cardinality/one}
   {:db/ident       :cart/line-items,
    :db/valueType   :db.type/ref,
    :db/cardinality :db.cardinality/many}])

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

(set! *warn-on-reflection* true)

(defn has-attr? [db eid attr]
  (-> db
      ^Iterable (d/datoms :eavt eid attr)
      .iterator
      .hasNext))

(defn pull-idents [db]
  (d/q
   '[:find [(pull ?e [*]) ...]
     :where [?e :db/ident]]
   db))

;; Datom helpers
(def -e (memfn ^datomic.Datom e))
(def -a (memfn ^datomic.Datom a))
(def -v (memfn ^datomic.Datom v))
(def -t (memfn ^datomic.Datom tx))
(def -added? (memfn ^datomic.Datom added))

;; Context helpers
(defn ctx-ident [ctx eid] (get-in ctx [:idents eid :db/ident]))
(defn ctx-valueType [ctx attr-id] (ctx-ident ctx (get-in ctx [:idents attr-id :db/valueType])))
(defn ctx-entid [ctx ident] (get-in ctx [:entids ident]))
(defn ctx-cardinality [ctx attr-id] (ctx-ident ctx (get-in ctx [:idents attr-id :db/cardinality])))
(defn ctx-card-many? [ctx attr-id] (= :db.cardinality/many
                                      (ctx-ident ctx (get-in ctx [:idents attr-id :db/cardinality]))))

(defn dash->underscore [s]
  (str/replace s #"-" "_"))

(defn table-name [ctx mem-attr]
  (get-in ctx
          [:tables mem-attr :name]
          (dash->underscore (namespace mem-attr))))

(defn join-table-name [ctx mem-attr val-attr]
  (get-in ctx
          [:tables mem-attr :rename-many-table val-attr]
          (dash->underscore (str (table-name ctx mem-attr) "_x_" (name val-attr)))))

(defn column-name [ctx mem-attr col-attr]
  (get-in ctx
          [:tables mem-attr :rename col-attr]
          (dash->underscore (name col-attr))))

(defn attr-db-type [ctx attr-id]
  (doto (get-in ctx [:db-types (ctx-valueType ctx attr-id)]) ))

;; Transaction processing logic
(defn track-idents [ctx tx-data]
  ;; Keep `:entids` and `:idents` up to date based on tx-data, this has some
  ;; shortcomings, but not sure yet about the general approach so will punt on
  ;; those.
  ;;
  ;; - cardinality/many is not properly handled, additional values simply
  ;;   replace the previous value(s) -> might not be a big issue because the
  ;;   attributes we care about are all cardinality/one
  ;;
  (let [db-ident  (get-in ctx [:entids :db/ident])
        tx-idents (filter #(= db-ident (-a %)) tx-data)
        tx-rest   (remove #(= db-ident (-a %)) tx-data)]
    (as-> ctx ctx
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

(defn encode-value [ctx type value]
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

(defn card-one-entity-ops [{:keys [tables] :as ctx} mem-attr eid datoms]
  (let [missing-cols (sequence
                      (comp
                       (remove (fn [d]
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
      (seq missing-cols)
      (-> (update :ops
                  (fnil conj [])
                  [:ensure-columns
                   {:table   (table-name ctx mem-attr)
                    :columns (into {} missing-cols)}])
          (update-in [:tables mem-attr :columns] (fnil into {}) missing-cols))
      :->
      (update :ops (fnil conj [])
              (if retracted?
                [:delete
                 {:table  (table-name ctx mem-attr)
                  :values {:db/id eid}}]
                [:upsert
                 {:table  (table-name ctx mem-attr)
                  :values (into {"db__id" eid}
                                (map (juxt #(column-name ctx mem-attr (ctx-ident ctx (-a %)))
                                           #(when (-added? %)
                                              (encode-value ctx
                                                            (ctx-valueType ctx (-a %))
                                                            (-v %)))))
                                datoms)}])))))

(defn card-many-entity-ops [{:keys [tables] :as ctx} mem-attr eid datoms]
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
                     {:table-name (join-table-name ctx mem-attr val-attr)
                      :fk-table (table-name ctx mem-attr)
                      :val-attr val-attr
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

(defn process-entity [{:keys [tables] :as ctx} db eid datoms]
  (reduce
   (fn [ctx [mem-attr table-opts]]
     (if (has-attr? db eid mem-attr)
       (let [datoms           (remove (fn [d] (contains? ignore-idents (ctx-ident ctx (-a d)))) datoms)
             card-one-datoms  (remove (fn [d] (ctx-card-many? ctx (-a d))) datoms)
             card-many-datoms (filter (fn [d] (ctx-card-many? ctx (-a d))) datoms)]
         (-> ctx
             (card-one-entity-ops mem-attr eid card-one-datoms)
             (card-many-entity-ops mem-attr eid card-many-datoms)))
       ctx))
   ctx
   tables))

(defn process-tx [ctx conn {:keys [t data]}]
  (let [ctx (track-idents ctx data)
        db (d/as-of (d/db conn) t)
        entities (group-by -e data)]
    (reduce (fn [ctx [eid datoms]]
              (process-entity ctx db eid datoms))
            ctx
            entities)))

(defmulti op->sql first)
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

(honey/format
 (-> (hh/alter-table :foo)
     (hh/add-column :skin :text :if-not-exists)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

@(d/transact conn schema)

@(d/transact conn [{:db/ident       :foo/baz
                    :db/valueType   :db.type/ref
                    :db/cardinality :db.cardinality/one}])

@(d/transact conn [[:db/retract :foo/bar :db/ident :foo/bar]])

(fd/create! conn cart)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def txs
  (into []
        (d/tx-range (d/log conn) nil nil)))

(def ctx
  ;; Bootstrap, make sure we have info about idents that datomic creates itself
  ;; at db creation time. d/as-of t=999 is basically an empty database with only
  ;; metaschema attributes (:db/txInstant etc), since the first "real"
  ;; transaction is given t=1000. Interesting to note that Datomic seems to
  ;; bootstrap in pieces: t=0 most basic idents, t=57 add double, t=63 add
  ;; docstrings, ...
  (let [idents (pull-idents (d/as-of (d/db conn) 999))]
    {:entids (into {} (map (juxt :db/ident :db/id)) idents)
     :idents (into {} (map (juxt :db/id identity)) idents)
     :tables (:tables metaschema)
     :db-types pg-type}))

(time
 (doall
  (:ops
   (reduce #(process-tx %1 conn %2) ctx txs))))

(time
 (let [txs (d/tx-range (d/log conn) nil nil)]
   (loop [ctx ctx
          [tx & txs] txs]
     (when tx
       (let [ctx (process-tx ctx conn tx)
             queries (eduction
                      (comp
                       (mapcat op->sql)
                       (map #(honey/format % {:quoted true})))
                      (:ops ctx))]
         (run! #(jdbc/execute! ds %) queries)
         (recur (dissoc ctx :ops) txs)
         )))))
(time
 (map op->sql
      (:ops
       (reduce #(process-tx %1 conn %2) ctx txs))))

(ctx-ident
 (reduce track-idents ctx (map :data txs))
 21)

(honey/format
 (-> (hh/create-table "foo" :if-not-exists)
     (hh/with-columns [[:db__id [:float 32] [:primary-key]]]))
 {:quoted true})

(jdbc/execute! ds
               ["INSERT INTO ? (\"db__id\", \"document_type_ref\", \"account_type_ref\", \"document_type_ref+account_type_ref\") VALUES (?, ?, ?, ?) ON CONFLICT (\"db__id\") DO UPDATE SET \"document_type_ref\" = EXCLUDED.\"document_type_ref\", \"account_type_ref\" = EXCLUDED.\"account_type_ref\", \"document_type_ref+account_type_ref\" = EXCLUDED.\"document_type_ref+account_type_ref\""
                "journal_entry_document_type"
                17592186045844
                17592186045736
                17592186045537
                "[17592186045537, 17592186045736]"])

(jdbc/on-connection
 [con ds]
 (-> (.getMetaData con) ; produces java.sql.DatabaseMetaData
     #_     (.getTables nil nil nil (into-array ["TABLE" "VIEW"]))
     (.getColumns nil nil "test" nil)
     (rs/datafiable-result-set ds nil)))
