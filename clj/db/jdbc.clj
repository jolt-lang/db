(ns db.jdbc
  "Entry point: loads the java.sql shim, then the real clojure.jdbc on top of it,
  then points clojure.jdbc's connection construction at the native drivers.

  Require this once before using jdbc.core. Two things depend on the order. The
  shim has to be loaded before clojure.jdbc's namespaces compile, because they
  resolve java.sql constants at compile time. And the IConnection extension below
  has to be loaded after clojure.jdbc's own, since the later extension is the one
  that wins, which is how a dbspec reaches db.sqlite / db.pg instead of
  DriverManager.

      (require '[db.jdbc])
      (require '[jdbc.core :as jdbc])
      (with-open [conn (jdbc/connection \"sqlite::memory:\")]
        (jdbc/fetch conn \"select 1 as one\"))

  Not wired in yet. While this repo still ships its own clj/jdbc/core.clj, that
  copy shadows the dependency on the classpath and the require below resolves to it
  rather than to clojure.jdbc, so this namespace only does what it says once that
  file is removed."
  (:require [db.jdbc-shim :as shim]
            [jdbc.proto :as proto]
            [jdbc.core]))

;; DriverManager and DataSource have nothing to load here, so a dbspec map or uri
;; string builds a shim connection over the native driver instead. Registered after
;; jdbc.impl's own extensions so these take precedence.
(extend-protocol proto/IConnection
  java.lang.String
  (connection [s] (shim/connection s))

  clojure.lang.IPersistentMap
  (connection [m] (shim/connection m)))
