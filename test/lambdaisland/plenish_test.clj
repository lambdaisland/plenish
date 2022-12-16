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

(defn import!
  ([metaschema]
   (import! metaschema nil))
  ([metaschema t]
   (let [ctx (plenish/initial-ctx *conn* metaschema t)]
     (plenish/import-tx-range
      ctx *conn* *ds*
      (d/tx-range (d/log *conn*) t nil)))))

(defn transact! [tx]
  @(d/transact *conn* tx))

(deftest basic-create-sync-test
  (fd/create! *conn* factories/cart)

  (import! factories/metaschema)

  (is (= {:users/email "arne@example.com"
          :users/email_confirmed? true}
         (jdbc/execute-one!
          *ds*
          ["SELECT email, \"email_confirmed?\" FROM users;"]))))

(deftest add-membership-after-attributes
  (let [fres    (fd/create! *conn* factories/cart {:traits [:not-created-yet]})
        cart-id (:db/id (f/sel1 fres factories/cart))
        user-id (:db/id (f/sel1 fres factories/user))]
    (transact! [{:db/id           cart-id
                 :cart/created-at #inst "2022-01-01T12:57:01.089-00:00"}])

    (import! factories/metaschema)

    (is (= {:cart/db__id     cart-id
            :cart/created_at (java.sql.Timestamp/valueOf "2022-01-01 12:57:01.089")
            :cart/age_ms     123.456
            :cart/user       user-id}
           (jdbc/execute-one! *ds* ["SELECT * FROM cart;"])))))

(deftest retract-attribute-test
  (let [fres    (fd/create! *conn* factories/cart)
        cart-id (:db/id (f/sel1 fres factories/cart))
        user-id (:db/id (f/sel1 fres factories/user))]

    (transact! [[:db/retract cart-id :cart/age-ms 123.456]])
    (import! factories/metaschema)

    (is (= {:cart/db__id     cart-id
            :cart/created_at (java.sql.Timestamp/valueOf "2022-06-23 12:57:01.089")
            :cart/user       user-id
            :cart/age_ms     nil}
           (jdbc/execute-one! *ds* ["SELECT * FROM cart;"])))
    (is (= [{:cart_x_line_items/db__id 17592186045418
             :cart_x_line_items/line_items 17592186045419}
            {:cart_x_line_items/db__id 17592186045418
             :cart_x_line_items/line_items 17592186045420}]
           (jdbc/execute! *ds* ["SELECT * FROM cart_x_line_items;"])))))

(deftest retract-entity-test
  (let [fres    (fd/create! *conn* factories/cart)
        cart-id (:db/id (f/sel1 fres factories/cart))
        user-id (:db/id (f/sel1 fres factories/user))]

    (transact! [[:db/retractEntity cart-id]])
    (import! factories/metaschema)

    (is (= [] (jdbc/execute! *ds* ["SELECT * FROM cart;"])))
    (is (= [] (jdbc/execute! *ds* ["SELECT * FROM cart_x_line_items;"])))))

(deftest ident-enum-test
  (testing "using a ref attribute and idents as an enum-type value"
    (transact! [{:db/ident :fruit/apple}
                {:db/ident :fruit/orange}
                {:db/ident :fruit/type
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/one}])
    (let [{:keys [tempids]} (transact! [{:db/id "apple"
                                         :fruit/type :fruit/apple}
                                        {:db/id "orange"
                                         :fruit/type :fruit/orange}])]
      (import! {:tables {:fruit/type {}}})

      (is (= [{:fruit/db__id (get tempids "apple")
               :idents/ident "fruit/apple"}
              {:fruit/db__id (get tempids "orange")
               :idents/ident "fruit/orange"}]
             (jdbc/execute! *ds* ["SELECT fruit.db__id, idents.ident FROM fruit, idents WHERE fruit.type = idents.db__id;"]))))))

(deftest update-cardinality-one-attribute--membership
  (testing "membership attribute"
    (transact! [{:db/ident :fruit/type
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one}])

    (let [tx-report (transact! [{:db/id "apple"
                                 :fruit/type "apple"}])
          {apple-id "apple"} (:tempids tx-report)]
      (transact! [{:db/id apple-id
                   :fruit/type "orange"}])

      (import! {:tables {:fruit/type {}}})
      (is (= [{:fruit/db__id apple-id
               :fruit/type "orange"}]
             (jdbc/execute! *ds* ["SELECT * FROM fruit"]))))))

(deftest update-cardinality-one-attribute--regular
  (testing "regular attribute"
    (transact! [{:db/ident :veggie/type
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident :veggie/rating
                 :db/valueType :db.type/long
                 :db/cardinality :db.cardinality/one}])

    (let [tx-report (transact! [{:db/id "brocolli"
                                 :veggie/type "brocolli"
                                 :veggie/rating 4}])
          {brocolli-id "brocolli"} (:tempids tx-report)]
      (transact! [{:db/id brocolli-id
                   :veggie/rating 5}])

      (import! {:tables {:veggie/type {}}})
      (is (= [{:veggie/db__id brocolli-id
               :veggie/type "brocolli"
               :veggie/rating 5}]
             (jdbc/execute! *ds* ["SELECT * FROM veggie"]))))))

(deftest update-cardinality-many-attribute
  ;; Does it make sense to have a cardinality/many attribute be the membership
  ;; attribute? Not sure. Punting on this for now.
  #_(testing "membership attribute"
      )

  (testing "regular attribute"
    (transact! [{:db/ident :veggie/type
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident :veggie/rating
                 :db/valueType :db.type/long
                 :db/cardinality :db.cardinality/many}])

    (let [tx-report (transact! [{:db/id "brocolli"
                                 :veggie/type "brocolli"
                                 :veggie/rating 4}])
          {brocolli-id "brocolli"} (:tempids tx-report)]
      (transact! [[:db/add brocolli-id :veggie/rating 5]
                  [:db/retract brocolli-id :veggie/rating 4]])

      (import! {:tables {:veggie/type {}}})
      (is (= [{:veggie_x_rating/db__id brocolli-id
               :veggie_x_rating/rating 5}]
             (jdbc/execute! *ds* ["SELECT * FROM veggie_x_rating"]))))))

(deftest duplicate-import-throws
  (testing "Trying to import a transaction that was already processed should throw"
    (fd/create! *conn* factories/cart)
    (import! factories/metaschema)

    (let [max-t (plenish/find-max-t *ds*)]
      (is (thrown? com.impossibl.postgres.jdbc.PGSQLIntegrityConstraintViolationException
                   (import! factories/metaschema max-t))))))

(comment
  ;; REPL alternative to fixture
  (recreate-replica!)
  (def *conn* (doto (d/connect
                     (doto (str "datomic:mem://" (gensym "tst"))
                       d/create-database))
                (d/transact factories/schema)))
  (def *ds* (jdbc/get-datasource "jdbc:pgsql://localhost:5432/replica?user=postgres"))

  (require 'kaocha.repl)
  (kaocha.repl/run)
  )
