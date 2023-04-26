(ns lambdaisland.mbrainz
  (:require
   [datomic.api :as d]
   [lambdaisland.plenish :as plenish]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]))

(def datomic-conn (d/connect "datomic:dev://localhost:4334/mbrainz-1968-1973"))
(def pg-conn (jdbc/get-datasource "jdbc:pgsql://localhost:5432/mbrainz?user=plenish&password=plenish"))

(def metaschema
  {:country/name {}
   :artist/gid {}
   :abstractRelease/gid {}
   :release/gid {}
   :medium/tracks {}
   :track/name {}})

(plenish/sync-to-latest datomic-conn pg-conn metaschema)
