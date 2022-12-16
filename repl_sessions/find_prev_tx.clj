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

;; Given we are currently processing a certain transaction (say t=1012), can we
;; find the t value of the previous transaction? This could be useful to add an
;; additional guarantee that transactions are processed exactly in order, by
;; adding a postgresql trigger that validates that transactions form an unbroken
;; chain.

(let [max-t 1012
      db (d/as-of (d/db conn) (dec max-t))]
  (d/q '[:find (max ?t) .
         :where
         [?i :db/txInstant]
         [(datomic.api/tx->t ?i) ?t]]
       db))
