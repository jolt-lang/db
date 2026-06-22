(ns next.jdbc
  "A next.jdbc compatibility layer for jolt, over jdbc.core (which binds the
  system db drivers via jolt.ffi). Just the surface migratus uses: get-connection,
  execute!, execute-batch!, and the with-transaction macro. See next.jdbc.sql for
  insert!/delete!/query and next.jdbc.prepare for statement batching.

  A connection is a tagged wrapper (a jolt.host/tagged-table) around a jdbc.core
  connection. It answers (instance? java.sql.Connection _) so migratus's
  do-commands runs SQL through its Connection branch, exposes the Connection
  methods migratus calls (setAutoCommit / isClosed / close / getMetaData), and
  unwraps to the raw jdbc.core conn for queries — all built from jolt core's
  generic host hooks (tagged-table / __register-class-methods! /
  __register-instance-check!), not from JDBC-specific core builtins."
  (:require [jdbc.core :as jc]
            [clojure.string :as str]))

(defn- product-name [vendor]
  (case vendor
    :sqlite "SQLite"
    :postgresql "PostgreSQL"
    (str vendor)))

(defn- normalize-spec
  "Map a next.jdbc-style db-spec onto what jdbc.core/connection accepts (a uri
  string or a {:vendor :name ...} map). Passes through strings and specs that
  jdbc.core already understands."
  [spec]
  (cond
    (string? spec) spec
    (map? spec)
    (cond
      (:connection-uri spec) (:connection-uri spec)
      (:jdbcUrl spec)        (str/replace-first (:jdbcUrl spec) "jdbc:" "")
      (:dbtype spec)         {:vendor (:dbtype spec)
                              :name   (or (:dbname spec) (:name spec) (:subname spec))}
      :else                  spec)
    :else spec))

;; A next.jdbc connection is a jolt tagged-table (jolt.host/tagged-table) around a
;; raw jdbc.core conn. It answers (instance? java.sql.Connection _) and the few
;; Connection methods migratus calls (setAutoCommit / isClosed / close /
;; getMetaData.getDatabaseProductName); conn-raw unwraps it for queries. Built
;; from jolt core's generic host hooks (no JDBC-specific builtins live in core).
(def ^:private conn-tag :next.jdbc/connection)
(def ^:private meta-tag :next.jdbc/dbmeta)

(clojure.core/__register-class-methods! conn-tag
  {"setAutoCommit" (fn [_ _] nil)
   "commit"        (fn [_] nil)
   "rollback"      (fn [_] nil)
   "isClosed"      (fn [self] (boolean (jolt.host/ref-get self :closed)))
   "close"         (fn [self]
                     (when-let [c (jolt.host/ref-get self :close)] (c))
                     (jolt.host/ref-put! self :closed true) nil)
   "getMetaData"   (fn [self]
                     (let [m (jolt.host/tagged-table meta-tag)]
                       (jolt.host/ref-put! m :product (jolt.host/ref-get self :product))
                       m))})
(clojure.core/__register-class-methods! meta-tag
  {"getDatabaseProductName" (fn [self] (jolt.host/ref-get self :product))})
(clojure.core/__register-instance-check!
  (fn [cn val]
    (if (= cn "java.sql.Connection")
      (and (jolt.host/table? val) (= (jolt.host/ref-get val :jolt/type) conn-tag))
      nil)))

(defn conn-raw
  "Unwrap a next.jdbc connection to the raw jdbc.core conn; pass anything else
  through (a plain jdbc.core conn map / spec)."
  [c]
  (if (and (jolt.host/table? c) (= (jolt.host/ref-get c :jolt/type) conn-tag))
    (jolt.host/ref-get c :raw)
    c))

(defn wrap-conn
  "Wrap a raw jdbc.core connection as a tagged next.jdbc connection."
  [raw]
  (let [t (jolt.host/tagged-table conn-tag)]
    (jolt.host/ref-put! t :raw raw)
    (jolt.host/ref-put! t :close (:close raw))
    (jolt.host/ref-put! t :product (product-name (:vendor raw)))
    t))

(defn get-connection
  "Open a connection from a db-spec, or return one that is already a wrapped
  connection (idempotent)."
  [spec]
  (if (instance? java.sql.Connection spec)
    spec
    (wrap-conn (jc/connection (normalize-spec spec)))))

(defn execute!
  "Run a statement (string or [sql & params]). Returns the jdbc.core result."
  ([conn q] (execute! conn q {}))
  ([conn q _opts] (jc/execute! (conn-raw conn) q)))

(defn execute-batch!
  "Run a batch of statements. next.jdbc has a richer contract; migratus only uses
  this for an idempotent schema upgrade, so run each command best-effort."
  ([conn sqls] (execute-batch! conn sqls nil nil))
  ([conn sqls _param-groups] (execute-batch! conn sqls nil nil))
  ([conn sqls _param-groups _opts]
   (let [raw (conn-raw conn)
         cmds (->> (if (sequential? sqls) sqls [sqls])
                   (mapcat (fn [s] (if (sequential? s) s [s]))))]
     (mapv (fn [s] (jc/execute! raw s)) cmds))))

(defmacro with-transaction
  "(with-transaction [t-con connectable] body...) — run body in a transaction.
  Delegates to jdbc.core/atomic-apply on the raw connection (nested calls become
  savepoints), binding the supplied symbol to a connection wrapping that same raw
  conn so body's queries and Statement batches run inside the transaction.

  Refs are fully qualified because jolt's syntax-quote does not resolve ns
  aliases (see jolt-9av)."
  [binding & body]
  (let [sym (first binding) connectable (second binding)]
    `(jdbc.core/atomic-apply
       (next.jdbc/conn-raw ~connectable)
       (fn [raw#]
         (let [~sym (next.jdbc/wrap-conn raw#)]
           ~@body)))))
