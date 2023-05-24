(ns lambdaisland.mbrainz
  (:require
   [datomic.api :as d]
   [lambdaisland.plenish :as plenish]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]))

(def datomic-conn (d/connect "datomic:dev://localhost:4334/mbrainz-1968-1973"))
(def duck-conn (jdbc/get-datasource "jdbc:duckdb:/tmp/mbrainz"))

(def metaschema
  {:tables {:release/name {}
            :artist/name {}}})

(def initial-ctx (plenish/initial-ctx datomic-conn metaschema))

(def new-ctx (plenish/import-tx-range
              initial-ctx datomic-conn duck-conn
              (d/tx-range (d/log datomic-conn) nil nil)))
