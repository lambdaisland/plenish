(ns repl-sessions.find-prev-tx
  (:require [lambdaisland.plenish :as plenish]
            [lambdaisland.plenish.factories :as factories]
            [lambdaisland.facai.datomic-peer :as fd]
            [datomic.api :as d]))

(set! *print-namespace-maps* false)

(def conn
  (let [url (str "datomic:mem://" (gensym "dev"))]
    (d/create-database url)
    (d/connect url)))

@(d/transact conn factories/schema)

(fd/create! conn factories/cart)

(seq
 (d/tx-range
  (d/log conn)
  1000
  9010))
