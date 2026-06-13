(ns next.jdbc.prepare
  "next.jdbc.prepare/statement over the host Statement shim. The returned
  statement accumulates SQL via .addBatch and runs them via .executeBatch using
  the connection's exec callback (so the batch runs inside the active
  transaction). Used by migratus's do-commands.")

(defn statement
  "Create a batch statement bound to a (wrapped) connection."
  [conn]
  (__jdbc-make-stmt conn))
