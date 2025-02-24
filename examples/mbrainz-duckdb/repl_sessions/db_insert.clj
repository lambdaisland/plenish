(ns db-insert
  (:require
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]))

(def duck-conn (jdbc/get-datasource "jdbc:duckdb:/tmp/mbrainz"))

(def cmd-a
  ["INSERT INTO \"artist\" (\"db__id\", \"sortName\", \"name\", \"type\", \"country\", \"gid\", \"startYear\") VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (\"db__id\") DO UPDATE SET \"sortName\" = EXCLUDED.\"sortName\", \"name\" = EXCLUDED.\"name\", \"type\" = EXCLUDED.\"type\", \"country\" = EXCLUDED.\"country\", \"gid\" = EXCLUDED.\"gid\", \"startYear\" = EXCLUDED.\"startYear\"" 778454232474826 "Deuter" "Deuter" 17592186045423 17592186045657 "c4e7031f-a5f0-476a-b1f0-1f3e8c573f4b" 1945])

(def cmd-b
  ["INSERT INTO \"artist\" (\"db__id\", \"sortName\", \"name\", \"type\", \"country\", \"gid\", \"startYear\") VALUES (?, ?, ?, ?, ?, ?, ?)"
   778454232474825 "Deuter" "Deuter" 17592186045423 17592186045657 "f81d4fae-7dec-11d0-a765-00a0c91e6bf6" 1945])

(jdbc/with-transaction [jdbc-tx duck-conn]
  (jdbc/execute! jdbc-tx cmd-a))
