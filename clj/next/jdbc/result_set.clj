(ns next.jdbc.result-set
  "Stub of the next.jdbc.result-set builder-fns. jdbc.core already returns
  unqualified lower-cased keyword-keyed row maps, so the builder-fn passed in
  query opts is just a marker that the sql layer ignores.")

(def as-unqualified-lower-maps :builder/unqualified-lower-maps)
(def as-unqualified-maps :builder/unqualified-maps)
(def as-lower-maps :builder/lower-maps)
