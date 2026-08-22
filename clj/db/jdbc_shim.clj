(ns db.jdbc-shim
  "The java.sql surface clojure.jdbc drives, over the native drivers in db.sqlite
  and db.pg, so the real clojure.jdbc runs on jolt unchanged rather than being
  reimplemented here. Registered through jolt's host-shim hooks
  (__register-class-statics! / __register-class-methods! /
  __register-instance-check!), the same way jolt-lang/http-client stands in for
  java.net.URL so clj-http-lite runs as published.

  Load order matters: clojure.jdbc's namespaces resolve the java.sql constants when
  they compile, so this namespace has to load before jdbc.core. Requiring db.jdbc
  does that in the right order.

  Shim objects are host tagged-tables whose fields are read and written with
  jolt.host/ref-get and ref-put!. Only the surface clojure.jdbc actually touches is
  implemented; anything else is deliberately absent so a gap shows up as a missing
  method rather than as a silently wrong answer."
  (:require [clojure.string :as str]
            [db.sqlite :as sqlite]
            [db.pg :as pg]))

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

;; --- shim object plumbing ----------------------------------------------------
(defn- tt [tag] (jolt.host/tagged-table tag))
(defn- tget [t k] (jolt.host/ref-get t k))
(defn- tput! [t k v] (jolt.host/ref-put! t k v))
(defn- table? [x] (jolt.host/table? x))

(defn- tagged? [x tag] (and (table? x) (= tag (tget x :jolt/type))))

(defn- sql-error
  "Throw as java.sql.SQLException by class, so a caller's (catch SQLException ...)
  matches on the class rather than on anything we put in ex-data."
  [msg]
  (throw (jolt.host/throwable "java.sql.SQLException" (str msg))))

;; The drivers report failures as ex-info carrying :jdbc/sql-error. Re-throw those
;; as typed SQLExceptions at the shim boundary so clojure.jdbc and its callers see
;; the class they expect.
(defn- as-sql-error [e]
  (if (:jdbc/sql-error (ex-data e))
    (sql-error (ex-message e))
    (throw e)))

(defmacro ^:private sql-try [& body]
  `(try ~@body (catch Exception e# (as-sql-error e#))))

;; --- driver-facing operations ------------------------------------------------
(defn- vendor [conn] (tget conn :vendor))
(defn- handle [conn] (tget conn :handle))

(defn- run-query
  "Execute `sql` and return {:labels [...] :rows [[v ...]]}."
  [conn sql params]
  (sql-try
    (case (vendor conn)
      :sqlite     (sqlite/query-raw (handle conn) sql params)
      :postgresql (pg/all-raw (handle conn) sql params))))

(defn- run-update
  "Execute `sql` and return the number of rows it affected."
  [conn sql params]
  (sql-try
    (case (vendor conn)
      :sqlite     (do (sqlite/query-raw (handle conn) sql params)
                      (sqlite/changes (handle conn)))
      :postgresql (pg/exec (handle conn) sql params))))

(defn execute-any
  "Execute `sql` ONCE and report both faces: {:labels [...] :rows [[...]]
  :count n}. A statement that returns a result set (SELECT, RETURNING) fills
  labels/rows; one that does not reports its update count. next.jdbc's
  execute!/execute-one!/plan sit on this, so they never run a statement twice
  to learn which kind it was."
  [conn sql params]
  (sql-try
    (case (vendor conn)
      :sqlite     (let [before (sqlite/total-changes (handle conn))
                        {:keys [labels rows]} (sqlite/query-raw (handle conn) sql params)]
                    {:labels labels :rows rows
                     ;; a total-changes delta, not changes(): changes() reads
                     ;; stale after DDL (it reports the last DML statement)
                     :count (if (seq labels) 0 (- (sqlite/total-changes (handle conn)) before))})
      :postgresql (pg/execute-any (handle conn) sql params))))

;; --- java.sql.ResultSetMetaData ----------------------------------------------
(defn- make-rsmeta [labels]
  (let [t (tt :jdbc/rsmeta)] (tput! t :labels labels) t))

(clojure.core/__register-class-methods! :jdbc/rsmeta
  {"getColumnCount" (fn [self] (count (tget self :labels)))
   ;; JDBC indexes columns from 1
   "getColumnLabel" (fn [self i] (nth (tget self :labels) (dec i)))
   "getColumnName"  (fn [self i] (nth (tget self :labels) (dec i)))})

;; --- java.sql.ResultSet ------------------------------------------------------
;; The drivers hand back every row at once, so this is a cursor over a vector
;; rather than a live server-side cursor. .next walks it; a fetch that streamed on
;; the JVM is eager here, which is a real difference and not just an internal one.
(defn- make-resultset [{:keys [labels rows]}]
  (let [t (tt :jdbc/resultset)]
    (tput! t :labels (or labels []))
    (tput! t :rows (or rows []))
    (tput! t :pos -1)
    (tput! t :closed false)
    t))

(defn- rs-current [self]
  (let [pos (tget self :pos) rows (tget self :rows)]
    (when (and (>= pos 0) (< pos (count rows))) (nth rows pos))))

(clojure.core/__register-class-methods! :jdbc/resultset
  {"next" (fn [self]
            (let [pos (inc (tget self :pos))]
              (tput! self :pos pos)
              (< pos (count (tget self :rows)))))
   "getMetaData" (fn [self] (make-rsmeta (tget self :labels)))
   "getObject" (fn [self i]
                 (let [row (or (rs-current self) (sql-error "ResultSet not positioned on a row"))]
                   (if (number? i)
                     (nth row (dec i))
                     ;; by label, case-insensitively, as JDBC does
                     (let [labels (tget self :labels)
                           idx (first (keep-indexed
                                       (fn [n l] (when (= (str/lower-case l)
                                                          (str/lower-case (str i))) n))
                                       labels))]
                       (if idx (nth row idx) (sql-error (str "no such column: " i)))))))
   "close" (fn [self] (tput! self :closed true) nil)
   "isClosed" (fn [self] (tget self :closed))})

;; --- java.sql.PreparedStatement ----------------------------------------------
;; Params arrive one at a time through .setObject at 1-based indexes, so collect
;; them in a map and flatten to a vector at execute time. That way a caller who
;; sets them out of order still gets them in order.
(defn- make-prepared [conn sql opts]
  (let [t (tt :jdbc/prepared)]
    (tput! t :conn conn)
    (tput! t :sql sql)
    (tput! t :params {})
    (tput! t :returning (:returning opts))
    (tput! t :max-rows (:max-rows opts))
    (tput! t :batch [])
    (tput! t :keys nil)
    (tput! t :closed false)
    t))

(defn- param-vec [self]
  (let [m (tget self :params)]
    (if (empty? m)
      []
      (mapv (fn [i] (get m i)) (range 1 (inc (apply max (keys m))))))))

(defn- limit-rows [self {:keys [labels rows]}]
  (let [n (tget self :max-rows)]
    {:labels labels :rows (if (and n (pos? n)) (vec (take n rows)) rows)}))

;; RETURNING is how the generated keys come back, since neither driver has a
;; JDBC-style generated-keys channel. :all / true asks for the whole row, which is
;; what postgres' own driver gives for RETURN_GENERATED_KEYS; a sequence of names
;; asks for those columns.
(defn- returning-sql [self]
  (let [r (tget self :returning)
        sql (str/trimr (str/replace (tget self :sql) #";\s*$" ""))]
    (cond
      (or (true? r) (= :all r)) (str sql " RETURNING *")
      (sequential? r)           (str sql " RETURNING "
                                     (str/join ", " (map name r)))
      :else                     nil)))

(clojure.core/__register-class-methods! :jdbc/prepared
  {"setObject" (fn [self i v] (tput! self :params (assoc (tget self :params) i v)) nil)
   "setString" (fn [self i v] (tput! self :params (assoc (tget self :params) i v)) nil)
   "setNull"   (fn [self i & _] (tput! self :params (assoc (tget self :params) i nil)) nil)

   "executeQuery" (fn [self]
                    (make-resultset
                     (limit-rows self (run-query (tget self :conn) (tget self :sql)
                                                 (param-vec self)))))

   "executeUpdate" (fn [self]
                     (let [conn (tget self :conn)
                           params (param-vec self)]
                       (if-let [rsql (returning-sql self)]
                         ;; run it as a query so the RETURNING rows can be handed
                         ;; back from getGeneratedKeys, and report the row count
                         (let [res (run-query conn rsql params)]
                           (tput! self :keys res)
                           (count (:rows res)))
                         (do (tput! self :keys nil)
                             (run-update conn (tget self :sql) params)))))

   ;; An empty ResultSet when nothing was requested is what makes insert! fall back
   ;; to the update count, which is how it behaves on a driver without generated
   ;; keys.
   "getGeneratedKeys" (fn [self]
                        (make-resultset (or (tget self :keys) {:labels [] :rows []})))

   "addBatch" (fn [self & _]
                (tput! self :batch (conj (tget self :batch) (param-vec self)))
                (tput! self :params {})
                nil)
   "executeBatch" (fn [self]
                    (let [conn (tget self :conn) sql (tget self :sql)]
                      (mapv (fn [ps] (run-update conn sql ps)) (tget self :batch))))

   "setQueryTimeout" (fn [self _] nil)
   "setFetchSize"    (fn [self _] nil)
   "setMaxRows"      (fn [self n] (tput! self :max-rows n) nil)
   "close"           (fn [self] (tput! self :closed true) nil)
   "isClosed"        (fn [self] (tget self :closed))})

;; --- java.sql.Statement ------------------------------------------------------
;; createStatement is only used for the no-parameter execute path, which batches a
;; single SQL string.
(defn- make-statement [conn]
  (let [t (tt :jdbc/statement)]
    (tput! t :conn conn) (tput! t :batch []) (tput! t :closed false) t))

(clojure.core/__register-class-methods! :jdbc/statement
  {"addBatch" (fn [self sql] (tput! self :batch (conj (tget self :batch) sql)) nil)
   "executeBatch" (fn [self]
                    (let [conn (tget self :conn)]
                      (mapv (fn [sql] (run-update conn sql [])) (tget self :batch))))
   "executeUpdate" (fn [self sql] (run-update (tget self :conn) sql []))
   "executeQuery" (fn [self sql] (make-resultset (run-query (tget self :conn) sql [])))
   "setQueryTimeout" (fn [self _] nil)
   "close" (fn [self] (tput! self :closed true) nil)})

;; --- java.sql.DatabaseMetaData -----------------------------------------------
(defn- make-dbmeta [conn]
  (let [t (tt :jdbc/dbmeta)] (tput! t :conn conn) t))

(clojure.core/__register-class-methods! :jdbc/dbmeta
  {"getDatabaseProductName" (fn [self]
                              (case (vendor (tget self :conn))
                                :sqlite "SQLite"
                                :postgresql "PostgreSQL"))
   "getConnection" (fn [self] (tget self :conn))})

;; --- java.sql.Connection -----------------------------------------------------
;; Transactions go through the same BEGIN / SAVEPOINT sequence the drivers already
;; understand. Autocommit off means a transaction is open, so BEGIN is issued on
;; the transition rather than eagerly.
(defn- make-connection [vendor handle close-fn]
  (let [t (tt :jdbc/connection)]
    (tput! t :vendor vendor)
    (tput! t :handle handle)
    (tput! t :close-fn close-fn)
    (tput! t :autocommit true)
    (tput! t :readonly false)
    (tput! t :isolation 2)                       ; TRANSACTION_READ_COMMITTED
    (tput! t :savepoints [])
    (tput! t :closed false)
    t))

(defn- exec! [conn sql] (run-update conn sql []))

(clojure.core/__register-class-methods! :jdbc/connection
  {"createStatement"  (fn [self & _] (make-statement self))
   ;; the overloads differ only in how generated keys are asked for: an int is
   ;; RETURN_GENERATED_KEYS, an array of names asks for those columns
   "prepareStatement" (fn [self sql & args]
                        (let [a (first args)]
                          (make-prepared self sql
                                         (cond
                                           (nil? a) {}
                                           (number? a) (if (= 1 a) {:returning :all} {})
                                           (sequential? a) {:returning (vec a)}
                                           :else {}))))

   "setAutoCommit" (fn [self v]
                     (let [v (boolean v)]
                       (when (not= v (tget self :autocommit))
                         (if v
                           (when-not (tget self :closed) (exec! self "COMMIT"))
                           (exec! self "BEGIN"))
                         (tput! self :autocommit v))
                       nil))
   "getAutoCommit" (fn [self] (tget self :autocommit))

   "commit" (fn [self]
              (exec! self "COMMIT")
              (when-not (tget self :autocommit) (exec! self "BEGIN"))
              nil)
   "rollback" (fn [self & [sp]]
                (if sp
                  (exec! self (str "ROLLBACK TO SAVEPOINT " (tget sp :name)))
                  (do (exec! self "ROLLBACK")
                      (when-not (tget self :autocommit) (exec! self "BEGIN"))))
                nil)

   "setSavepoint" (fn [self & [nm]]
                    (let [n (count (tget self :savepoints))
                          name (or nm (str "jdbc_sp_" n))
                          sp (tt :jdbc/savepoint)]
                      (tput! sp :name name)
                      (tput! self :savepoints (conj (tget self :savepoints) name))
                      (exec! self (str "SAVEPOINT " name))
                      sp))
   "releaseSavepoint" (fn [self sp]
                        (exec! self (str "RELEASE SAVEPOINT " (tget sp :name)))
                        nil)

   "setReadOnly" (fn [self v] (tput! self :readonly (boolean v)) nil)
   "isReadOnly"  (fn [self] (tget self :readonly))
   "setTransactionIsolation" (fn [self v] (tput! self :isolation v) nil)
   "getTransactionIsolation" (fn [self] (tget self :isolation))
   "setSchema" (fn [self s]
                 (when (and s (= :postgresql (vendor self)))
                   (exec! self (str "SET search_path TO " s)))
                 nil)

   "getMetaData" (fn [self] (make-dbmeta self))
   "isClosed" (fn [self] (tget self :closed))
   "close" (fn [self]
             (when-not (tget self :closed)
               ((tget self :close-fn))
               (tput! self :closed true))
             nil)})

(clojure.core/__register-class-methods! :jdbc/savepoint
  {"getSavepointName" (fn [self] (tget self :name))})

;; --- instance? / catch -------------------------------------------------------
;; clojure.jdbc dispatches protocols on these classes and uses with-open, so the
;; shim values have to answer instance? for them.
(def ^:private class-tags
  {"java.sql.Connection"       :jdbc/connection
   "java.sql.PreparedStatement" :jdbc/prepared
   "java.sql.Statement"        :jdbc/statement
   "java.sql.ResultSet"        :jdbc/resultset
   "java.sql.ResultSetMetaData" :jdbc/rsmeta
   "java.sql.DatabaseMetaData" :jdbc/dbmeta
   "java.sql.Savepoint"        :jdbc/savepoint})

;; Answer true or nil, never false. nil means "not one of mine, keep looking",
;; while false settles the question for every other library's check as well: the
;; first non-nil answer wins. next.jdbc registers its own check so its connection
;; wrapper answers instance? java.sql.Connection, which is how migratus picks its
;; Connection branch, and returning false here silently overruled it.
(clojure.core/__register-instance-check!
  (fn [cn val]
    (when-let [tag (get class-tags cn)]
      ;; a PreparedStatement is a Statement too
      (when (or (tagged? val tag)
                (and (= cn "java.sql.Statement") (tagged? val :jdbc/prepared)))
        true))))

;; Report the java.sql class name for (class x) and, more importantly, so a
;; protocol extended to java.sql.Connection dispatches on these values.
;; clojure.jdbc extends IConnection to java.sql.Connection returning `this`, and
;; without this that arm never fires.
(def ^:private tag->class
  (into {} (map (fn [[c t]] [t c]) class-tags)))

(clojure.core/__register-class!
  (fn [x] (and (table? x) (contains? tag->class (tget x :jolt/type))))
  (fn [x] (get tag->class (tget x :jolt/type)))
  (fn [x] (let [c (get tag->class (tget x :jolt/type))]
            (if (= c "java.sql.PreparedStatement")
              ["java.sql.PreparedStatement" "java.sql.Statement"]
              [c]))))

;; --- connection construction -------------------------------------------------
(defn- sqlite-connection [name]
  (let [h (sqlite/open name)]
    (sqlite/query-raw h "PRAGMA foreign_keys=1;" [])
    (make-connection :sqlite h (fn [] (sqlite/close h)))))

(defn- pg-connection [uri]
  (let [h (pg/connect uri)]
    (make-connection :postgresql h (fn [] (pg/close h)))))

(defn- pg-uri [{:keys [subname host port user password dbname] :as spec}]
  (let [;; subname is JDBC's //host:port/db
        sn (or subname "")
        sn (if (str/starts-with? sn "//") (subs sn 2) sn)
        [hostport db] (let [i (str/index-of sn "/")]
                        (if i [(subs sn 0 i) (subs sn (inc i))] ["" sn]))
        [db qs] (let [i (str/index-of (or db "") "?")]
                  (if i [(subs db 0 i) (subs db (inc i))] [db nil]))
        params (when qs
                 (into {} (map (fn [kv]
                                 (let [[k v] (str/split kv #"=" 2)] [k v]))
                               (str/split qs #"&"))))
        user (or user (get params "user"))
        password (or password (get params "password"))]
    (str "postgres://"
         (when user (str user (when password (str ":" password)) "@"))
         (if (str/blank? hostport) (str (or host "127.0.0.1")
                                        (when port (str ":" port)))
             hostport)
         "/" (or (when-not (str/blank? db) db) dbname (:name spec)))))

(defn connection
  "Open a java.sql.Connection shim for a clojure.jdbc dbspec. Recognises the
  classic :subprotocol/:subname form, the pretty :vendor/:name form, and a uri
  string; anything else is not a spec this library can serve."
  [spec]
  (cond
    (tagged? spec :jdbc/connection) spec

    (string? spec)
    (let [s spec]
      (cond
        (str/starts-with? s "postgres") (pg-connection s)
        (str/starts-with? s "sqlite:")  (sqlite-connection (subs s 7))
        :else                           (sqlite-connection s)))

    (map? spec)
    (let [v (or (:subprotocol spec) (:vendor spec))
          v (str/lower-case (str v))]
      (cond
        (contains? #{"postgresql" "postgres" "pgsql"} v) (pg-connection (pg-uri spec))
        (contains? #{"sqlite" "sqlite3"} v)
        (sqlite-connection (let [n (or (:subname spec) (:name spec))]
                             (if (str/starts-with? (str n) "//") (subs (str n) 2) (str n))))
        :else (sql-error (str "unsupported vendor for this driver: " v))))

    :else (sql-error (str "invalid dbspec: " (pr-str spec)))))
