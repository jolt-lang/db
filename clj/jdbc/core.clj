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
    ;; bare path = sqlite file
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

;; the postgres driver is reached through pgfn, defined with the other pg helpers
;; below; connection calls it for the :postgresql branch.
(declare pgfn)

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

;;; ? -> $N rewriting
;;
;; Which ? counts as a placeholder decides which parameter lands where, so a ?
;; that only looks like one has to be skipped without consuming a number. That
;; means recognising the constructs a ? can hide in: string literals, quoted
;; identifiers, line and block comments, dollar-quoted bodies, and E'' escape
;; strings. Each is skipped whole by the scanner below.

(defn- at
  "The character at `i`, or nil past the end, so callers can compare without
  bounds-checking first."
  [sql len i]
  (when (< i len) (nth sql i)))

(defn- digit? [c] (let [x (int c)] (and (>= x 48) (<= x 57))))

(defn- ident-char? [c]
  (let [x (int c)]
    (or (and (>= x 97) (<= x 122))              ; a-z
        (and (>= x 65) (<= x 90))               ; A-Z
        (and (>= x 48) (<= x 57))               ; 0-9
        (= x 95)                                ; _
        (> x 127))))                            ; postgres allows non-ascii here

(defn- token-start?
  "True when `i` begins a token rather than continuing an identifier. Postgres
  allows $ inside an identifier after the first character, so a$b$c is one name
  and not a dollar quote opening, and E only introduces an escape string when it
  stands alone rather than ending a word like date'2020-01-01'. Its own lexer
  draws the line in the same place."
  [sql i]
  (or (zero? i)
      (let [p (nth sql (dec i))]
        (not (or (ident-char? p) (= p \$))))))

(defn- skip-quoted
  "Index just past the run of quote character `q` opening at `i`. A doubled quote
  is an escaped one; `escapes?` additionally honours backslash escapes, which is
  what distinguishes E'...' from a plain literal. An unterminated run ends at the
  end of the statement rather than throwing."
  [sql len i q escapes?]
  (loop [j (inc i)]
    (cond
      (>= j len)                        len
      (and escapes? (= (nth sql j) \\)) (recur (+ j 2))
      (not= (nth sql j) q)              (recur (inc j))
      (= (at sql len (inc j)) q)        (recur (+ j 2))
      :else                             (inc j))))

(defn- skip-line-comment [sql len i]
  (loop [j (+ i 2)]
    (cond (>= j len)                 len
          (= (nth sql j) \newline)   j
          :else                      (recur (inc j)))))

(defn- skip-block-comment
  "Index just past the /* */ comment opening at `i`. Postgres nests these, so
  track depth rather than stopping at the first */."
  [sql len i]
  (loop [j (+ i 2) depth 1]
    (cond
      (>= j len) len
      (and (= (nth sql j) \/) (= (at sql len (inc j)) \*)) (recur (+ j 2) (inc depth))
      (and (= (nth sql j) \*) (= (at sql len (inc j)) \/)) (if (= depth 1)
                                                             (+ j 2)
                                                             (recur (+ j 2) (dec depth)))
      :else (recur (inc j) depth))))

(defn- dollar-tag-len
  "Length of the $tag$ that opens at `i`, or nil when this $ does not open a
  dollar quote. The tag follows unquoted-identifier rules, so it cannot start
  with a digit, which is what keeps a positional $1 from being read as one."
  [sql len i]
  (loop [j (inc i)]
    (let [c (at sql len j)]
      (cond
        (nil? c)                        nil
        (= c \$)                        (- (inc j) i)
        (and (= j (inc i)) (digit? c))  nil
        (ident-char? c)                 (recur (inc j))
        :else                           nil))))

(defn- skip-dollar-quoted [sql len i taglen]
  (let [tag (subs sql i (+ i taglen))]
    (if-let [close (str/index-of sql tag (+ i taglen))]
      (+ close taglen)
      len)))

(defn- pg-placeholders
  "JDBC ? placeholders -> postgres $1..$N. A ? inside a string literal, quoted
  identifier, comment, dollar-quoted body or escape string is left as it is and
  does not consume a number. Collects the pieces and joins them once, so the cost
  is linear in the length of the statement."
  [sql]
  (let [len (count sql)]
    (loop [i 0 from 0 pnum 1 pieces (transient [])]
      (if (>= i len)
        (str/join (persistent! (conj! pieces (subs sql from len))))
        (let [c (nth sql i)
              nxt (at sql len (inc i))]
          (cond
            (= c \?)
            (recur (inc i) (inc i) (inc pnum)
                   (conj! (conj! pieces (subs sql from i)) (str "$" pnum)))

            (= c \') (recur (skip-quoted sql len i \' false) from pnum pieces)
            (= c \") (recur (skip-quoted sql len i \" false) from pnum pieces)

            ;; E'...' / e'...', where a backslash escapes the next character
            (and (or (= c \E) (= c \e)) (= nxt \') (token-start? sql i))
            (recur (skip-quoted sql len (inc i) \' true) from pnum pieces)

            (and (= c \-) (= nxt \-)) (recur (skip-line-comment sql len i) from pnum pieces)
            (and (= c \/) (= nxt \*)) (recur (skip-block-comment sql len i) from pnum pieces)

            (= c \$)
            (if-let [taglen (and (token-start? sql i) (dollar-tag-len sql len i))]
              (recur (skip-dollar-quoted sql len i taglen) from pnum pieces)
              (recur (inc i) from pnum pieces))

            :else (recur (inc i) from pnum pieces)))))))

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
       :postgresql (pg-eval conn sql params)))))

(defn last-insert-id
  "Driver-specific id of the last inserted row (sqlite: last_insert_rowid,
  postgres: lastval, which needs the session to have used a sequence)."
  [conn]
  (case (:vendor conn)
    :sqlite     (sqlite/last-insert-rowid (:handle conn))
    :postgresql (:id (first ((pgfn "all") (:handle conn) "select lastval() as id" [])))))

;;; insert! / update! / delete! — the clojure.jdbc convenience surface

(defn- entity-str [entities x] (entities (if (keyword? x) (name x) (str x))))

(defn insert!
  "Insert one row map and return the generated id, from last_insert_rowid on
  sqlite and lastval on postgres. Inserting into a postgres table with no
  sequence therefore throws, since lastval has nothing to report — use
  \"... returning ...\" with execute!/fetch for that case."
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

;; SQL errors satisfy (catch java.sql.SQLException ...) — migratus's
;; table-exists? probe and friends rely on that contract.
(clojure.core/__register-instance-check!
  (fn [cn val]
    (if (= cn "java.sql.SQLException")
      (boolean (:jdbc/sql-error (ex-data val)))
      nil)))
