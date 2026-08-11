(ns db.jdbc-shim
  "The java.sql surface clojure.jdbc drives, over the native drivers in db.sqlite
  and db.pg, so the real clojure.jdbc runs on jolt unchanged rather than being
  reimplemented here. Registered through jolt's host-shim hooks
  (__register-class-statics! / __register-class-ctor! / __register-class-methods! /
  __register-instance-check!), the same way jolt-lang/http-client stands in for
  java.net.URL so clj-http-lite runs as published.

  Load order matters: clojure.jdbc's namespaces resolve these classes when they
  compile, so this namespace has to be loaded before jdbc.core. Requiring db.jdbc
  does that in the right order.

  Shim objects are host tagged-tables whose fields are read and written with
  jolt.host/ref-get and ref-put!.")

;; --- java.sql constants ------------------------------------------------------
;; jdbc.constants maps its keyword options onto these, so they have to read as the
;; same ints the JVM uses: a caller who passes :serializable through to
;; setTransactionIsolation gets 8 either way.

(clojure.core/__register-class-statics! "java.sql.ResultSet"
  {"TYPE_FORWARD_ONLY"        1003
   "TYPE_SCROLL_INSENSITIVE"  1004
   "TYPE_SCROLL_SENSITIVE"    1005
   "CONCUR_READ_ONLY"         1007
   "CONCUR_UPDATABLE"         1008
   "HOLD_CURSORS_OVER_COMMIT" 1
   "CLOSE_CURSORS_AT_COMMIT"  2
   "FETCH_FORWARD"            1000
   "FETCH_REVERSE"            1001
   "FETCH_UNKNOWN"            1002})

(clojure.core/__register-class-statics! "java.sql.Connection"
  {"TRANSACTION_NONE"             0
   "TRANSACTION_READ_UNCOMMITTED" 1
   "TRANSACTION_READ_COMMITTED"   2
   "TRANSACTION_REPEATABLE_READ"  4
   "TRANSACTION_SERIALIZABLE"     8})

(clojure.core/__register-class-statics! "java.sql.Statement"
  {"RETURN_GENERATED_KEYS" 1
   "NO_GENERATED_KEYS"     2
   "CLOSE_CURRENT_RESULT"  1
   "KEEP_CURRENT_RESULT"   2
   "CLOSE_ALL_RESULTS"     3
   "SUCCESS_NO_INFO"       -2
   "EXECUTE_FAILED"        -3})
