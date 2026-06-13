(ns next.jdbc.transaction
  "next.jdbc.transaction knobs. migratus binds *nested-tx* to control nested
  transaction behavior; jdbc.core handles nesting with savepoints, so this is
  just a recognized dynamic var.")

(def ^:dynamic *nested-tx* :allow)
