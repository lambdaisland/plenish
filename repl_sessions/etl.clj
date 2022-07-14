(ns repl-sessions.etl
  (:require [lambdaisland.plenish :as plenish]
            [datomic.api :as d]
            [next.jdbc :as jdbc]))

;; CREATE DATABASE enzo;
;; CREATE ROLE plenish WITH LOGIN PASSWORD 'plenish';
;; GRANT ALL ON DATABASE enzo TO plenish;

(def metaschema
  (read-string (slurp "/home/arne/Eleven/runeleven/melvn/presto-etc/datomic/accounting.edn")))

(defn recreate-db! [name]
  (let [ds (jdbc/get-datasource "jdbc:pgsql://localhost:5432/postgres?user=postgres")]
    (jdbc/execute! ds [(str "DROP DATABASE IF EXISTS " name)])
    (jdbc/execute! ds [(str "CREATE DATABASE " name)])))

(defn import! [datomic-url jdbc-url]
  (let [conn (d/connect datomic-url)
        ds (jdbc/get-datasource jdbc-url)
        txs (d/tx-range (d/log conn) nil nil)]
    (plenish/import-tx-range
     (plenish/initial-ctx conn metaschema)
     conn
     ds
     txs)))

(recreate-db! "enzo")

(import! "datomic:dev://localhost:4334/example-tenant---enzo-gardening-service-limited-demo-"
         "jdbc:pgsql://localhost:5432/enzo?user=postgres")

(def conn
  (d/connect
   "datomic:dev://localhost:4334/example-tenant---enzo-gardening-service-limited-demo-"
   #_
   "datomic:dev://localhost:4334/example-tenant---camelot-global-trust-pte-ltd")
  )

(d/q
 '[:find (pull ?e [*])
   :where (or
           [?e :accounting.fiscal-year/closing-description]
           [?e :accounting.account/bank-account-name]
           [?e :accounting.account/bank-account-number]
           [?e :accounting.account/description])]
 (d/db conn))


(seq
 (d/q
  '[:find [?e ...]
    :where [_ :db/ident ?e]]
  (d/db conn)))

;; datomic:dev://localhost:4334/tnt
;; datomic:dev://localhost:4334/onze
;; datomic:dev://localhost:4334/example-tenant---camelot-global-trust-pte-ltd-
;; datomic:dev://localhost:4334/example-tenant---enzo-gardening-service-limited-demo-

:accounting.fiscal-year/closing-description
:accounting.account/bank-account-name
:accounting.account/bank-account-number
:accounting.account/description

(d/entid (d/db conn) :accounting.account/description);; => 183
(d/entid (d/db conn) :accounting.account/fiscal-year-ref+number);; => 232
(d/entid (d/db conn) :accounting.account/bank-account-number);; => 181

(map :t
     (filter
      (fn [tx]
        (some #(#{181} (.-a %)) (:data tx)))
      (seq
       (d/tx-range
        (d/log conn)
        nil
        nil))))

(rand-nth (seq
           (d/tx-range
            (d/log conn)
            nil
            nil)))


(d/pull (d/db conn) '[*] 17592186045433 #_13194139534819)
(map
 d/tx->t
 (d/q
  '[:find [?tx ...]
    :where [_ :accounting.account/description _ ?tx]]
  (d/db conn)))
