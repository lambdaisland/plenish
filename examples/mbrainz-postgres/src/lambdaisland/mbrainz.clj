(ns lambdaisland.mbrainz
  (:require
   [datomic.api :as d]
   [lambdaisland.plenish :as plenish]
   [lambdaisland.plenish.adapters.postgres :as postgres]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]))

(def datomic-conn (d/connect "datomic:dev://localhost:4334/mbrainz-1968-1973"))
(def pg-conn (jdbc/get-datasource "jdbc:pgsql://localhost:5432/mbrainz?user=plenish&password=plenish"))

(def metaschema
  {:tables {:release/name {}
            :artist/name {}}})

(def db-adapter (postgres/db-adapter))

(def initial-ctx (plenish/initial-ctx datomic-conn metaschema db-adapter))

(def new-ctx (plenish/import-tx-range
              initial-ctx datomic-conn pg-conn
              (d/tx-range (d/log datomic-conn) nil nil)))
