(ns shadow.grove.db.ident-protocol)

(defprotocol IdentProtocol
  (-make-ident [type id])
  (-ident? [thing])
  (-ident-key [thing])
  (-ident-val [thing])
  (-ident-as-vec [thing]))
