(ns repl-sessions.jdbc-poke
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [honey.sql :as honey]
            [honey.sql.helpers :as hh]))

(def ds
  (jdbc/get-datasource
   "jdbc:pgsql://localhost:5432/replica?user=plenish&password=plenish"
   ))

(jdbc/)

(jdbc/execute!
 ds
 (honey/format
  (-> (hh/create-table "foo" :if-not-exists)
      (hh/with-columns [[:db__id :bigint [:primary-key]]]))))


(jdbc/on-connection
 [con ds]
 (-> (.getMetaData con) ; produces java.sql.DatabaseMetaData
     #_     (.getTables nil nil nil (into-array ["TABLE" "VIEW"]))
     (.getColumns nil nil "test" nil)
     (rs/datafiable-result-set ds nil)))
