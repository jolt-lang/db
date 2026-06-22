(ns next.jdbc
  "A next.jdbc compatibility layer for jolt, over jdbc.core (which binds the
  system db drivers via jolt.ffi). Just the surface migratus uses: get-connection,
  execute!, execute-batch!, and the with-transaction macro. See next.jdbc.sql for
  insert!/delete!/query and next.jdbc.prepare for statement batching.

  A connection is a tagged wrapper (built by the __jdbc-* host builtins) around a
  jdbc.core connection. It answers (instance? java.sql.Connection _) so migratus's
  do-commands runs SQL through its Connection branch, carries a clj :exec callback
  the host-side Statement.executeBatch invokes, and unwraps to the raw jdbc.core
  conn for queries."
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

(defn wrap-conn
  "Wrap a raw jdbc.core connection as a tagged next.jdbc connection."
  [raw]
  (__jdbc-wrap-conn raw
                    (fn [sql] (jc/execute! raw sql))
                    (:close raw)
                    (product-name (:vendor raw))))

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
  ([conn q _opts] (jc/execute! (__jdbc-conn-raw conn) q)))

(defn execute-batch!
  "Run a batch of statements. next.jdbc has a richer contract; migratus only uses
  this for an idempotent schema upgrade, so run each command best-effort."
  ([conn sqls] (execute-batch! conn sqls nil nil))
  ([conn sqls _param-groups] (execute-batch! conn sqls nil nil))
  ([conn sqls _param-groups _opts]
   (let [raw (__jdbc-conn-raw conn)
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
       (__jdbc-conn-raw ~connectable)
       (fn [raw#]
         (let [~sym (next.jdbc/wrap-conn raw#)]
           ~@body)))))
