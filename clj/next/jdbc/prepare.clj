(ns next.jdbc.prepare
  "next.jdbc.prepare/statement — a batch statement bound to a (wrapped) connection.
  The statement accumulates SQL via .addBatch and runs them via .executeBatch on
  the connection (so the batch runs inside the active transaction). Built from
  jolt's generic host hooks (tagged-table + __register-class-methods!), not core."
  (:require [next.jdbc :as njdbc]))

(def ^:private stmt-tag :next.jdbc/statement)

(clojure.core/__register-class-methods! stmt-tag
  {"addBatch"     (fn [self sql]
                    (jolt.host/ref-put! self :batch
                                        (conj (or (jolt.host/ref-get self :batch) []) sql))
                    nil)
   "executeBatch" (fn [self]
                    ;; JDBC's int[] of per-statement update counts. execute!
                    ;; answers the upstream row/update-count shape now, so read
                    ;; the count back out of it.
                    (let [conn (jolt.host/ref-get self :conn)
                          sqls (or (jolt.host/ref-get self :batch) [])
                          res  (mapv (fn [s]
                                       (let [r (njdbc/execute! conn s)]
                                         (or (:next.jdbc/update-count (first r))
                                             (count r))))
                                     sqls)]
                      (jolt.host/ref-put! self :batch [])
                      res))
   "clearBatch"   (fn [self] (jolt.host/ref-put! self :batch []) nil)
   "close"        (fn [_] nil)})

(defn statement
  "Create a batch statement bound to a (wrapped) connection."
  [conn]
  (let [t (jolt.host/tagged-table stmt-tag)]
    (jolt.host/ref-put! t :conn conn)
    (jolt.host/ref-put! t :batch [])
    t))
