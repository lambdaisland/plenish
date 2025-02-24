(ns lambdaisland.plenish.adapters.duckdb
  "The DuckDB adapter"
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
        :db.type/instant [:raw (format "epoch_ms(%d)" (inst-ms value))]
        :db.type/uuid (str value)
        value))))


