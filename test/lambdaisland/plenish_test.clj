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

(comment
  (def *conn* (doto (d/connect
                     (doto (str "datomic:mem://" (gensym "tst"))
                       d/create-database))
                (d/transact factories/schema)))
  (def *ds* (jdbc/get-datasource "jdbc:pgsql://localhost:5432/replica?user=postgres")))

(fd/create! *conn* factories/cart)


(def ctx
  (plenish/initial-ctx *conn*
                       {:tables {:line-item/price {}
                                 :cart/created-at {}
                                 :user/uuid {:name "users"}}}))

(def new-ctx
  (plenish/import-tx-range ctx *conn* *ds*
                           (d/tx-range (d/log *conn*) nil nil))
  )

(count (mapcat :data (d/tx-range (d/log *conn*) nil nil)))

(seq
 (d/tx-range (d/log *conn*) nil nil))

(d/q '[:find (pull ?e [*])
       :where [?e :user/uuid]]
     (d/db *conn*))
