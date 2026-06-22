(ns jdbc.core
  "clojure.jdbc's API (https://github.com/yogthos/clojure.jdbc) for jolt,
  over native database drivers (bound through jolt.ffi) instead of java.sql:

  - SQLite via db.sqlite (the system libsqlite3)
  - PostgreSQL via db.pg (the system libpq)

  Connections are plain maps carrying the driver handle plus a :close fn, so
  `with-open` works. Queries are strings or sqlvecs ([sql & params], JDBC ?
  placeholders — rewritten to $N for postgres). Rows come back as vectors of
  keyword-keyed maps.

      (require '[jdbc.core :as jdbc])
      (with-open [conn (jdbc/connection \"sqlite::memory:\")]
        (jdbc/execute! conn \"create table p (id integer primary key, name text)\")
        (jdbc/insert! conn :p {:name \"ada\"})
        (jdbc/fetch conn [\"select * from p where name = ?\" \"ada\"]))"
  (:require [clojure.string :as str]
            [db.sqlite :as sqlite]))

;;; dbspec

(defn- parse-uri-spec [s]
  (cond
    (str/starts-with? s "postgres")  {:vendor "postgresql" :uri s}
    (str/starts-with? s "sqlite:")   {:vendor "sqlite" :name (subs s 7)}
    ;; bare path = sqlite file, matching the db janet library's convention
    :else                            {:vendor "sqlite" :name s}))

(defn- pg-uri [{:keys [uri name host port user password]}]
  (or uri
      (str "postgres://"
           (when user (str user (when password (str ":" password)) "@"))
           (or host "127.0.0.1")
           (when port (str ":" port))
           "/" name)))

(defn- normalize-spec [spec]
  (cond
    (string? spec) (parse-uri-spec spec)
    (map? spec)    (let [vendor (or (:vendor spec) (:subprotocol spec))
                         spec   (assoc spec :vendor (case vendor
                                                      ("postgresql" "postgres" "pgsql") "postgresql"
                                                      ("sqlite" "sqlite3") "sqlite"
                                                      (throw (ex-info (str "unknown vendor: " vendor) {:spec spec}))))]
                     (if (:subname spec) (assoc spec :name (:subname spec)) spec))
    :else (throw (ex-info "dbspec must be a string or a map" {:spec spec}))))

;;; connection

(defn connection
  "Open a connection. spec is a uri string (\"sqlite:path\", a bare sqlite
  path, or \"postgres://user:pass@host:port/db\") or a dbspec map with
  :vendor (or :subprotocol) + :name/:subname [:host :port :user :password].
  The returned conn map has a :close fn — use with-open."
  [spec]
  (let [{:keys [vendor] :as spec} (normalize-spec spec)]
    (case vendor
      "sqlite"
      (let [h (sqlite/open (:name spec))]
        (sqlite/query h "PRAGMA foreign_keys=1;" [])
        {:vendor   :sqlite
         :handle   h
         :depth    (atom 0)
         :rollback (atom false)
         :close    (fn [] (sqlite/close h))})
      "postgresql"
      ;; db.pg (and libpq) load lazily — only when a postgres connection is made,
      ;; so a sqlite-only app never needs libpq present.
      (do (require '[db.pg])
          (let [h ((pgfn "connect") (pg-uri spec))]
            {:vendor   :postgresql
             :handle   h
             :depth    (atom 0)
             :rollback (atom false)
             :close    (fn [] ((pgfn "close") h))})))))

;;; queries

(defn- sqlvec [q]
  (cond
    (string? q) [q []]
    (vector? q) [(first q) (vec (rest q))]
    :else (throw (ex-info "query must be a string or sqlvec" {:q q}))))

(defn- pg-placeholders
  "JDBC ? placeholders -> postgres $1..$N (skipping ? inside '...' literals)."
  [sql]
  (loop [out "" i 0 n 1 in-str false]
    (if (= i (count sql))
      out
      (let [c (subs sql i (inc i))]
        (cond
          (= c "'") (recur (str out c) (inc i) n (not in-str))
          (and (= c "?") (not in-str)) (recur (str out "$" n) (inc i) (inc n) in-str)
          :else (recur (str out c) (inc i) n in-str))))))

(defn- sqlite-eval [conn sql params]
  (sqlite/query (:handle conn) sql params))

;; db.pg is required lazily (only for a postgres connection), so resolve its fns
;; at runtime — a compile-time db.pg/foo reference would be read as a host class.
(defn- pgfn [n] (deref (resolve (symbol "db.pg" n))))

(defn- pg-eval [conn sql params]
  ((pgfn "exec") (:handle conn) (pg-placeholders sql) params))

(defn fetch
  "Run a query (string or sqlvec), return a vector of keyword-keyed row maps."
  ([conn q] (fetch conn q {}))
  ([conn q opts]
   (let [[sql params] (sqlvec q)
         rows (case (:vendor conn)
                :sqlite     (sqlite-eval conn sql params)
                :postgresql ((pgfn "all") (:handle conn) (pg-placeholders sql) params))]
     (if-let [n (:max-rows opts)] (vec (take n rows)) rows))))

(defn fetch-one
  "Run a query, return the first row map (or nil)."
  ([conn q] (fetch-one conn q {}))
  ([conn q opts] (first (fetch conn q (merge {:max-rows 1} opts)))))

(defn execute!
  "Execute a statement (string or sqlvec). Returns rows affected."
  ([conn q] (execute! conn q {}))
  ([conn q opts]
   (let [[sql params] (sqlvec q)]
     (case (:vendor conn)
       :sqlite     (do (sqlite-eval conn sql params)
                       (sqlite/changes (:handle conn)))
       :postgresql (do (pg-eval conn sql params) nil)))))

(defn last-insert-id
  "Driver-specific id of the last inserted row (sqlite: last_insert_rowid)."
  [conn]
  (case (:vendor conn)
    :sqlite     (sqlite/last-insert-rowid (:handle conn))
    :postgresql (:id (first ((pgfn "all") (:handle conn) "select lastval() as id" [])))))

;;; insert! / update! / delete! — the clojure.jdbc convenience surface

(defn- entity-str [entities x] (entities (if (keyword? x) (name x) (str x))))

(defn insert!
  "Insert one row map. Returns the generated id (sqlite) / nil (postgres —
  use \"... returning *\" with execute!/fetch for the row)."
  ([conn table row] (insert! conn table row {}))
  ([conn table row opts]
   (let [entities (get opts :entities identity)
         cols (vec (keys row))
         sql (str "INSERT INTO " (entity-str entities table)
                  " (" (str/join ", " (map #(entity-str entities %) cols)) ")"
                  " VALUES (" (str/join ", " (repeat (count cols) "?")) ")")]
     (execute! conn (into [sql] (map #(get row %) cols)) opts)
     (last-insert-id conn))))

(defn insert-multi!
  "Insert a sequence of row maps; returns a vector of generated ids."
  ([conn table rows] (insert-multi! conn table rows {}))
  ([conn table rows opts]
   (mapv #(insert! conn table % opts) rows)))

(defn update!
  "(update! conn :person {:zip 94540} [\"zip = ?\" 94546])"
  ([conn table set-map where-clause] (update! conn table set-map where-clause {}))
  ([conn table set-map where-clause opts]
   (let [entities (get opts :entities identity)
         cols (vec (keys set-map))
         [where & wparams] where-clause
         sql (str "UPDATE " (entity-str entities table)
                  " SET " (str/join ", " (map #(str (entity-str entities %) " = ?") cols))
                  (when-not (str/blank? (or where "")) (str " WHERE " where)))]
     (execute! conn (into (into [sql] (map #(get set-map %) cols)) wparams) opts))))

(defn delete!
  "(delete! conn :person [\"zip = ?\" 94546])"
  ([conn table where-clause] (delete! conn table where-clause {}))
  ([conn table where-clause opts]
   (let [entities (get opts :entities identity)
         [where & params] where-clause
         sql (str "DELETE FROM " (entity-str entities table)
                  (when-not (str/blank? (or where "")) (str " WHERE " where)))]
     (execute! conn (into [sql] params) opts))))

;;; transactions: BEGIN at depth 0, SAVEPOINTs when nested (both drivers).

(defn set-rollback!
  "Mark the current transaction to roll back at the end of the atomic block."
  [conn]
  (reset! (:rollback conn) true)
  conn)

(defn atomic-apply
  "Run (func conn) in a transaction; nested calls use savepoints."
  ([conn func] (atomic-apply conn func {}))
  ([conn func opts]
   (let [depth @(:depth conn)
         sp (str "jdbc_sp_" depth)
         begin (if (zero? depth) "BEGIN" (str "SAVEPOINT " sp))
         commit (if (zero? depth) "COMMIT" (str "RELEASE SAVEPOINT " sp))
         rollback (if (zero? depth) "ROLLBACK" (str "ROLLBACK TO SAVEPOINT " sp))]
     (execute! conn begin)
     (swap! (:depth conn) inc)
     (try
       (let [ret (func conn)]
         (swap! (:depth conn) dec)
         (if (and (zero? @(:depth conn)) @(:rollback conn))
           (do (reset! (:rollback conn) false)
               (execute! conn rollback))
           (execute! conn commit))
         ret)
       (catch Throwable t
         (swap! (:depth conn) dec)
         (execute! conn rollback)
         (when (zero? @(:depth conn)) (reset! (:rollback conn) false))
         (throw t))))))

(defmacro atomic
  "(atomic conn body...) — body runs in a transaction bound to conn."
  [conn & body]
  (if (map? (first body))
    `(atomic-apply ~conn (fn [c#] (let [~conn c#] ~@(next body))) ~(first body))
    `(atomic-apply ~conn (fn [c#] (let [~conn c#] ~@body)))))
