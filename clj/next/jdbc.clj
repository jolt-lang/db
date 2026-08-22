(ns next.jdbc
  "A next.jdbc compatibility layer for jolt, over jdbc.core — which is
  clojure.jdbc itself, running on the java.sql shim in db.jdbc-shim above the
  native drivers.

  The surface: get-datasource / get-connection, execute! / execute-one! / plan,
  execute-batch! (the upstream contract: one SQL, a seq of parameter groups),
  and with-transaction with its options map. Every operation takes a
  connectable — an open connection, a datasource (opened and closed per call,
  next.jdbc's ownership rule), or a db-spec. Rows are unqualified lower-cased
  keyword maps — the drivers cannot see table names, so the qualified builders
  upstream defaults to are not reproducible; next.jdbc.result-set's builder
  markers are accepted and ignored.

  A connection is a tagged wrapper (a jolt.host/tagged-table) around a
  jdbc.core connection. It answers (instance? java.sql.Connection _) so
  migratus's do-commands runs SQL through its Connection branch, exposes the
  Connection methods migratus calls, and unwraps to the raw jdbc.core conn for
  queries — all built from jolt core's generic host hooks."
  (:require [db.jdbc]                       ; loads the shim BEFORE jdbc.core compiles
            [db.jdbc-shim :as shim]
            [jdbc.core :as jc]
            [jdbc.proto :as proto]
            [db.datasource :as ds]
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
  through (a plain jdbc.core conn)."
  [c]
  (if (and (jolt.host/table? c) (= (jolt.host/ref-get c :jolt/type) conn-tag))
    (jolt.host/ref-get c :raw)
    c))

(defn wrap-conn
  "Wrap a raw jdbc.core connection as a tagged next.jdbc connection. The
  jdbc.core value is a reify, so its vendor comes off the SHIM connection
  behind it and close goes through its own Closeable — reading (:vendor raw) /
  (:close raw) off the reify answered nil, which left the product name empty
  and .close a no-op that never closed the driver handle."
  [raw]
  (let [t (jolt.host/tagged-table conn-tag)
        shim-conn (proto/connection raw)]
    (jolt.host/ref-put! t :raw raw)
    (jolt.host/ref-put! t :close (fn [] (.close raw)))
    (jolt.host/ref-put! t :product (product-name (jolt.host/ref-get shim-conn :vendor)))
    t))

(defn- shim-of
  "The db.jdbc-shim connection behind a (wrapped or raw) connection."
  [c]
  (proto/connection (conn-raw c)))

(defn get-datasource
  "spec -> datasource (db.datasource). Idempotent on datasources."
  [spec]
  (if (ds/datasource? spec) spec (ds/open-datasource (normalize-spec spec))))

(defn get-connection
  "Open a connection from a db-spec or a datasource, or return one that is
  already a wrapped connection (idempotent)."
  [spec]
  (cond
    (instance? java.sql.Connection spec) spec
    (ds/datasource? spec) (wrap-conn (ds/acquire spec))
    :else (wrap-conn (jc/connection (normalize-spec spec)))))

(defn- call-with-connection
  "Run f with a wrapped connection for `connectable`. A datasource or a bare
  spec opens a connection OWNED by this call (closed on the way out); an
  existing connection is used as-is and left open."
  [connectable f]
  (cond
    (instance? java.sql.Connection connectable) (f connectable)
    (or (ds/datasource? connectable) (string? connectable) (map? connectable))
    (let [w (get-connection connectable)]
      (try (f w) (finally (.close w))))
    :else (f (wrap-conn connectable))))          ; a raw jdbc.core connection

(defn- sqlvec [q] (if (sequential? q) [(first q) (vec (rest q))] [q []]))
(defn- rows->maps [labels rows]
  (let [ks (mapv keyword labels)]
    (mapv (fn [vs] (zipmap ks vs)) rows)))

(defn execute!
  "Run a statement ([sql & params] or a string) against a connectable. Answers
  the row maps when the statement returns a result set, and
  [{:next.jdbc/update-count n}] otherwise — the upstream shape."
  ([connectable q] (execute! connectable q {}))
  ([connectable q _opts]
   (call-with-connection connectable
     (fn [w]
       (let [[sql params] (sqlvec q)
             {:keys [labels rows count]} (shim/execute-any (shim-of w) sql params)]
         (if (seq labels)
           (rows->maps labels rows)
           [{:next.jdbc/update-count count}]))))))

(defn execute-one!
  "Like execute!, but answers the FIRST row map (nil when the result set is
  empty), or {:next.jdbc/update-count n} for a statement with no result set."
  ([connectable q] (execute-one! connectable q {}))
  ([connectable q _opts]
   (call-with-connection connectable
     (fn [w]
       (let [[sql params] (sqlvec q)
             {:keys [labels rows count]} (shim/execute-any (shim-of w) sql params)]
         (if (seq labels)
           (when-let [r (first rows)] (zipmap (mapv keyword labels) r))
           {:next.jdbc/update-count count}))))))

(defn plan
  "A reducible over the rows of `q`. The drivers hand back every row at once
  (documented in db.jdbc-shim), so this reduces an already-materialized vector
  — the value of plan here is the calling convention, not streaming. Rows are
  plain unqualified keyword maps."
  ([connectable q] (plan connectable q {}))
  ([connectable q _opts]
   (reify clojure.lang.IReduceInit
     (reduce [_ f init]
       (call-with-connection connectable
         (fn [w]
           (let [[sql params] (sqlvec q)
                 {:keys [labels rows]} (shim/execute-any (shim-of w) sql params)]
             (reduce f init (rows->maps labels rows)))))))))

(defn- update-count-of [w sql params]
  (:count (shim/execute-any (shim-of w) sql params)))

(defn execute-batch!
  "The upstream contract: one SQL statement run across `param-groups` (a seq of
  parameter vectors) inside the current transaction, answering the per-group
  update counts. The 2-arity also keeps this library's original migratus shape
  — a seq of SQL strings run in order — which predates the upstream contract
  here; param-groups used to be accepted and silently DISCARDED, which is the
  bug this replaces."
  ([connectable sqls]
   (if (and (sequential? sqls) (every? string? sqls))
     (call-with-connection connectable
       (fn [w] (mapv (fn [s] (update-count-of w s [])) sqls)))
     (throw (ex-info "execute-batch!: expected a seq of SQL strings, or (execute-batch! conn sql param-groups)"
                     {:arg sqls}))))
  ([connectable sql param-groups] (execute-batch! connectable sql param-groups {}))
  ([connectable sql param-groups _opts]
   (when (sequential? sql)
     (throw (ex-info "execute-batch!: sql must be a single statement; param groups carry the rows"
                     {:sql sql})))
   (call-with-connection connectable
     (fn [w] (mapv (fn [group] (update-count-of w sql (vec group))) param-groups)))))

(defn transact*
  "Run (f tx-connection) in a transaction on the connectable. Options:
  :isolation (a next.jdbc keyword, becomes clojure.jdbc's :isolation-level),
  :read-only, :rollback-only. Nested calls become savepoints (jdbc.core)."
  [connectable f opts]
  (call-with-connection connectable
    (fn [w]
      (jc/atomic-apply
       (conn-raw w)
       (fn [raw]
         (when (:rollback-only opts) (jc/set-rollback! raw))
         (f (wrap-conn raw)))
       (cond-> {}
         (:isolation opts) (assoc :isolation-level (:isolation opts))
         (contains? opts :read-only) (assoc :read-only (:read-only opts)))))))

(defmacro with-transaction
  "(with-transaction [t-con connectable opts?] body...) — run body in a
  transaction. The connectable may be a connection, a datasource (a connection
  is opened for the transaction and closed after), or a spec.

  Refs are fully qualified because jolt's syntax-quote does not resolve ns
  aliases (see jolt-9av)."
  [binding & body]
  (let [sym (first binding) connectable (second binding) opts (nth binding 2 nil)]
    `(next.jdbc/transact* ~connectable (fn [~sym] ~@body) ~opts)))
