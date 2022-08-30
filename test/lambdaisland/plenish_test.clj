(ns lambdaisland.plenish-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [lambdaisland.facai :as f]
            [lambdaisland.plenish :as plenish]
            [lambdaisland.plenish.factories :as factories]
            [lambdaisland.facai.datomic-peer :as fd]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(def ^:dynamic *conn* "Datomic connection" nil)
(def ^:dynamic *ds* "JDBC datasource" nil)

;; docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres

(defn recreate-replica! []
  (let [ds (jdbc/get-datasource "jdbc:pgsql://localhost:5432/postgres?user=postgres")]
    (jdbc/execute! ds ["DROP DATABASE IF EXISTS replica;"])
    (jdbc/execute! ds ["CREATE DATABASE replica;"])))

(use-fixtures :each
  (fn [f]
    (recreate-replica!)
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

    (is (= {:users/email "arne@example.com"
            :users/email_confirmed? true}
           (jdbc/execute-one!
            *ds*
            ["SELECT email, \"email_confirmed?\" FROM users;"])))))

(deftest add-membership-after-attributes
  (let [fres    (fd/create! *conn* factories/cart {:traits [:not-created-yet]})
        cart-id (:db/id (f/sel1 fres factories/cart))
        user-id (:db/id (f/sel1 fres factories/user))
        ctx     (plenish/initial-ctx *conn* factories/metaschema)]
    (d/transact *conn* [{:db/id           cart-id
                         :cart/created-at #inst "2022-01-01T12:57:01.089-00:00"}])
    (plenish/import-tx-range ctx *conn* *ds* (d/tx-range (d/log *conn*) nil nil))

    (is (= {:cart/db__id     cart-id
            :cart/created_at (java.sql.Timestamp/valueOf "2022-01-01 12:57:01.089")
            :cart/age_ms     123.456
            :cart/user       user-id}
           (jdbc/execute-one! *ds* ["SELECT * FROM cart;"])))))

(deftest retract-attribute-test
  (let [fres    (fd/create! *conn* factories/cart)
        cart-id (:db/id (f/sel1 fres factories/cart))
        user-id (:db/id (f/sel1 fres factories/user))
        ctx     (plenish/initial-ctx *conn* factories/metaschema)]
    (d/transact *conn* [[:db/retract cart-id :cart/age-ms 123.456]])
    (plenish/import-tx-range ctx *conn* *ds* (d/tx-range (d/log *conn*) nil nil))

    (is (= {:cart/db__id     cart-id
            :cart/created_at (java.sql.Timestamp/valueOf "2022-06-23 12:57:01.089")
            :cart/user       user-id
            :cart/age_ms     nil}
           (jdbc/execute-one! *ds* ["SELECT * FROM cart;"])))
    (is (= [{:cart_x_line_items/db__id 17592186045418, :cart_x_line_items/line_items 17592186045420}]
           (jdbc/execute! *ds* ["SELECT * FROM cart_x_line_items;"])))))

(deftest retract-entity-test
  (let [fres    (fd/create! *conn* factories/cart)
        cart-id (:db/id (f/sel1 fres factories/cart))
        user-id (:db/id (f/sel1 fres factories/user))
        ctx     (plenish/initial-ctx *conn* factories/metaschema)]
    (d/transact *conn* [[:db/retractEntity cart-id]])
    (plenish/import-tx-range ctx *conn* *ds* (d/tx-range (d/log *conn*) nil nil))

    (is (= [] (jdbc/execute! *ds* ["SELECT * FROM cart;"])))
    (is (= [] (jdbc/execute! *ds* ["SELECT * FROM cart_x_line_items;"])))))


(comment
  ;; REPL alternative to fixture
  (recreate-replica!)
  (def *conn* (doto (d/connect
                     (doto (str "datomic:mem://" (gensym "tst"))
                       d/create-database))
                (d/transact factories/schema)))
  (def *ds* (jdbc/get-datasource "jdbc:pgsql://localhost:5432/replica?user=postgres"))

  )
