(ns lambdaisland.plenish.factories
  "Factories so we can easily populate some test dbs"
  (:require [lambdaisland.facai :as f]))

(f/defactory line-item
  {:line-item/description "Widgets"
   :line-item/quantity 5
   :line-item/price 1.0M
   :line-item/category :items
   :line-item/foo 123})

(f/defactory user
  {:user/email "arne@example.com"
   :user/email-confirmed? true
   :user/uuid #uuid "f7ea3cda-9fbb-4af4-9b2f-db72a3b57781"
   :user/avatar (byte-array [1 2 3 4])
   :user/homepage (java.net.URI. "http://example.com")})

(f/defactory cart
  {:cart/created-at #inst "2022-06-23T12:57:01.089-00:00"
   :cart/age-ms 123.456
   :cart/line-items [line-item line-item]
   :cart/user user}

  :traits
  {:not-created-yet
   {:after-build
    (fn [ctx]
      (f/update-result ctx dissoc :cart/created-at))}})

(defn s
  "More concise Datomic schema notation"
  [[sname type {:as opts}]]
  (merge
   {:db/ident sname
    :db/valueType (keyword "db.type" (name type))
    :db/cardinality :db.cardinality/one}
   opts))

(def schema
  (map
   s
   [[:line-item/description :string]
    [:line-item/quantity :long]
    [:line-item/price :bigdec]
    [:line-item/category :keyword]
    [:user/email :string]
    [:user/email-confirmed? :boolean]
    [:user/uuid :uuid]
    [:user/avatar :bytes]
    [:user/homepage :uri]
    [:cart/created-at :instant]
    [:cart/age-ms :double]
    [:cart/line-items :ref {:db/cardinality :db.cardinality/many}]
    [:cart/user :ref]]))

(def metaschema
  {:tables {:line-item/price {}
            :cart/created-at {}
            :user/uuid {:name "users"}}})
