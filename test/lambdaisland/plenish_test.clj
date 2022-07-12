(ns lambdaisland.plenish-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [lambdaisland.plenish :as plenish]
            [lambdaisland.plenish.factories :as factories]
            [lambdaisland.facai.datomic-peer :as fd]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(def ^:dynamic *conn* "Datomic connection" nil)
(def ^:dynamic *ds* "JDBC datasource" nil)

;; docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres

(use-fixtures :each
  (fn [f]
    (let [ds (jdbc/get-datasource "jdbc:pgsql://localhost:5432/postgres?user=postgres")]
      (jdbc/execute! ds ["DROP DATABASE IF EXISTS replica;"])
      (jdbc/execute! ds ["CREATE DATABASE replica;"]))
    (binding [*conn* (d/connect (doto (str "datomic:mem://" (gensym "tst")) d/create-database))
              *ds* (jdbc/get-datasource "jdbc:pgsql://localhost:5432/replica?user=postgres")]
      @(d/transact *conn* factories/schema)
      (f))))

(deftest basic-create-sync-test
  (fd/create! *conn* factories/cart)

  (let [ctx (plenish/initial-ctx *conn* factories/metaschema)]
    (plenish/import-tx-range
     ctx *conn* *ds*
     (d/tx-range (d/log *conn*) nil nil))

    (is (= {:user/email "arne@example.com"
            :user/email-confirmed? true}
           (d/q '[:find (pull ?e [:user/email :user/email-confirmed?]) .
                  :where [?e :user/uuid]]
                (d/db *conn*))))))

(comment
  (def *conn* (doto (d/connect
                     (doto (str "datomic:mem://" (gensym "tst"))
                       d/create-database))
                (d/transact factories/schema)))
  (def *ds* (jdbc/get-datasource "jdbc:pgsql://localhost:5432/replica?user=postgres")))
