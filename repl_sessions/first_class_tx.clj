(ns repl-sessions.first-class-tx
  (:require [lambdaisland.plenish :as plenish]
            [datomic.api :as d]
            [next.jdbc :as jdbc]))

(def metaschema
  (read-string (slurp "/home/arne/Eleven/runeleven/melvn/presto-etc/datomic/accounting.edn")))

(def conn
  (let [url (str "datomic:mem://" (gensym "dev"))]
    (d/create-database url)
    (d/connect url)))

(defn pg-url [db-name]
  (str "jdbc:pgsql://localhost:5432/" db-name "?user=postgres"))

(defn recreate-db! [name]
  (let [ds (jdbc/get-datasource (pg-url "postgres"))]
    (jdbc/execute! ds [(str "DROP DATABASE IF EXISTS " name)])
    (jdbc/execute! ds [(str "CREATE DATABASE " name)])))

(defn import! [metaschema datomic-url jdbc-url]
  (let [conn (d/connect datomic-url)
        ds (jdbc/get-datasource jdbc-url)
        txs (d/tx-range (d/log conn) nil nil)]
    (plenish/import-tx-range
     (plenish/initial-ctx conn metaschema)
     conn
     ds
     txs)))

(def conn (d/connect "datomic:dev://localhost:4334/example-tenant---import-company-20210901T110746698441277"))

(plenish/ctx-valueType (plenish/initial-ctx conn metaschema)
                       50)

(get-in (plenish/initial-ctx conn metaschema) [:idents 50 :db/valueType] )
(map :db/valueType (vals (:idents (plenish/initial-ctx conn metaschema))))

(recreate-db! "foo4")

(import!
 metaschema
 "datomic:dev://localhost:4334/example-tenant---import-company-20210901T110746698441277"
 (pg-url "foo4"))

(#'plenish/pull-idents (d/as-of (d/db conn) 999))

;; (d/entid (d/db conn) :db/txInstant)

(d/pull (d/db conn) '[*] 25)

(def ctx (plenish/initial-ctx conn metaschema))

(let [[t1000 t1009] (seq
                     (d/tx-range
                      (d/log conn)
                      1000
                      1010))]
  (into #{} (map plenish/-e) (:data t1009))
  #_(-> ctx
        (plenish/process-tx conn t1000)
        (plenish/process-tx conn t1009)))
(seq
 (d/tx-range
  (d/log conn)
  1026
  1027))

;; datomic:dev://localhost:4334/example-tenant---import-company-20210901T105828757108918
;; datomic:dev://localhost:4334/example-tenant---import-company-20210901T110147221074714
;; datomic:dev://localhost:4334/example-tenant---import-company-20210901T110526995513023
;; datomic:dev://localhost:4334/example-tenant---import-company-20210901T110623353849568
;; datomic:dev://localhost:4334/example-tenant---import-company-20210901T110713392203667
;; datomic:dev://localhost:4334/example-tenant---import-company-20210901T110746698441277
