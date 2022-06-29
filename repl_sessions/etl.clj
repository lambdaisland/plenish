(ns repl-sessions.etl
  (:require [lambdaisland.plenish :as plenish]
            [datomic.api :as d]
            [next.jdbc :as jdbc]))

"datomic:dev://localhost:4334/tnt"
"datomic:dev://localhost:4334/onze"


;; CREATE DATABASE enzo;
;; CREATE ROLE plenish WITH LOGIN PASSWORD 'plenish';
;; GRANT ALL ON DATABASE enzo TO plenish;

(def metaschema
  (read-string (slurp "/home/arne/runeleven/melvn/presto-etc/datomic/accounting.edn")))

(defn import! [datomic-url jdbc-url]
  (let [conn (d/connect datomic-url)
        ds (jdbc/get-datasource jdbc-url)
        txs (d/tx-range (d/log conn) nil nil)]
    (plenish/import-tx-range
     (plenish/initial-ctx conn metaschema)
     conn
     ds
     txs)))

(import! "datomic:dev://localhost:4334/example-tenant---camelot-global-trust-pte-ltd"
         "jdbc:pgsql://localhost:5432/camelot?user=plenish&password=plenish")

(import! "datomic:dev://localhost:4334/example-tenant---enzo-gardening-service-limited-demo-"
         "jdbc:pgsql://localhost:5432/enzo?user=plenish&password=plenish")
