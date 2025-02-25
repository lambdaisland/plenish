(ns lambdaisland.plenish.adapters.postgres
  "The postgres adapter"
  (:require [charred.api :as charred]
            [clojure.string :as str]
            [lambdaisland.plenish.protocols :as proto]
            [lambdaisland.plenish :as plenish]))

(defn db-adapter []
  (reify proto/IDatomicEncoder
    (encode-value [_ ctx type value]
      (case type
        :db.type/ref (if (keyword? value)
                       (plenish/ctx-entid ctx value)
                       value)
        :db.type/tuple [:raw (str \' (str/replace (str (charred/write-json-str value)) "'" "''") \' "::jsonb")]
        :db.type/keyword (str (when (qualified-ident? value)
                                (str (namespace value) "/"))
                              (name value))
        :db.type/instant [:raw (format "to_timestamp(%.3f)" (double (/ (.getTime ^java.util.Date value) 1000)))]
        value))
    (db-type [_]
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
       :db.type/tuple :jsonb})))
