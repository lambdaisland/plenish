(ns lambdaisland.plenish.protocols
  "Main database abstractions defined as protocols")

(defprotocol IDatomicEncoder
  "Protocol for encoding Datomic values based on target DB."
  (encode-value [this ctx type value] "Encode a value based on Datomic type and target DB."))
