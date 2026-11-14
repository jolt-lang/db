(ns next-jdbc-test
  "The next.jdbc compatibility surface: execute-one!, plan, execute-batch!'s
  real contract, datasources, with-transaction options, and the PG value
  model. Called from jdbc.core-test/-main; shares its check/failures plumbing."
  (:require [jdbc.core :as jc]
            [next.jdbc :as nj]
            [next.jdbc.sql :as sql]
            [db.datasource :as ds]))

(defn run [check]
  (println "next.jdbc surface over sqlite (:memory:)")
  (let [conn (nj/get-connection "sqlite::memory:")]
    (nj/execute! conn "create table t (id integer primary key, x integer)")

    (check "execute-batch! runs one sql across param groups" [1 1 1]
           (nj/execute-batch! conn "insert into t (x) values (?)" [[1] [2] [3]] {}))
    (check "execute-batch! rows landed" 3
           (count (sql/query conn "select * from t")))
    (check "execute-batch! legacy seq-of-sql shape still runs" [0 0]
           (nj/execute-batch! conn ["create table l1 (a integer)" "create table l2 (a integer)"]))

    (check "execute-one! answers the first row" {:id 1 :x 1}
           (nj/execute-one! conn ["select * from t order by id"]))
    (check "execute-one! answers an update count for DML" {:next.jdbc/update-count 1}
           (nj/execute-one! conn ["insert into t (x) values (?)" 9]))
    (check "execute-one! answers nil for an empty result" nil
           (nj/execute-one! conn ["select * from t where x = ?" 12345]))

    (check "execute! answers rows for a select" [{:one 1}]
           (nj/execute! conn ["select 1 as one"]))

    ;; next.jdbc's insert! answers the generated keys, not an update count.
    (nj/execute! conn "create table ins (id integer primary key, name text)")
    (check "sql/insert! answers the generated keys" {:id 1 :name "ada"}
           (sql/insert! conn :ins {:name "ada"}))
    (check "sql/insert! {:return-keys false} answers the update count"
           {:next.jdbc/update-count 1}
           (sql/insert! conn :ins {:name "bo"} {:return-keys false}))
    (check "execute! answers an update-count row for DML" [{:next.jdbc/update-count 1}]
           (nj/execute! conn ["insert into t (x) values (?)" 10]))

    (check "plan reduces without materializing a seq" 6
           (reduce (fn [acc row] (+ acc (:x row))) 0
                   (nj/plan conn ["select x from t where x <= ?" 3])))
    (check "plan transduces" [2 3 4]
           (transduce (map (comp inc :x)) conj []
                      (nj/plan conn ["select x from t where x <= 3 order by x"])))
    (check "plan honors reduced" 1
           (reduce (fn [_ row] (reduced (:x row))) nil
                   (nj/plan conn ["select x from t order by x"])))

    (check "getMetaData names the product" "SQLite"
           (.getDatabaseProductName (.getMetaData conn)))

    (println "transactions with options")
    (nj/with-transaction [tx conn {:rollback-only true}]
      (sql/query tx "select 1"))
    (check "with-transaction accepts an options map" true true)
    (nj/with-transaction [tx conn {:rollback-only true}]
      (nj/execute! tx ["insert into t (x) values (?)" 777]))
    (check ":rollback-only discards the block" 0
           (count (sql/query conn ["select * from t where x = ?" 777])))
    (nj/with-transaction [tx conn {:isolation :serializable}]
      (nj/execute! tx ["insert into t (x) values (?)" 778]))
    (check ":isolation is accepted and the block commits" 1
           (count (sql/query conn ["select * from t where x = ?" 778])))

    (check ".close closes the underlying handle" :closed
           (do (.close conn)
               (try (sql/query conn "select 1") :still-open
                    (catch Exception _ :closed)))))

  (println "next.jdbc.sql update!/find-by-keys/get-by-id")
  (let [conn (nj/get-connection "sqlite::memory:")]
    (nj/execute! conn "create table p (id integer primary key, name text, age integer, city text)")
    (doseq [r [{:name "ada" :age 36 :city "london"}
               {:name "bo" :age 20 :city "paris"}
               {:name "cy" :age 50 :city "london"}
               {:name "di" :age 41 :city nil}]]
      (sql/insert! conn :p r))
    (check "update! with a map where answers the update count" {:next.jdbc/update-count 2}
           (sql/update! conn :p {:age 99} {:city "london"}))
    (check "update! with a vector where" {:next.jdbc/update-count 1}
           (sql/update! conn :p {:city "rome"} ["name = ?" "bo"]))
    (check "update! with nil in the where map matches IS NULL" {:next.jdbc/update-count 1}
           (sql/update! conn :p {:city "oslo"} {:city nil}))
    (check "find-by-keys with a map" ["ada" "cy"]
           (mapv :name (sql/find-by-keys conn :p {:age 99} {:order-by [:id]})))
    (check "find-by-keys with a vector where" ["bo"]
           (mapv :name (sql/find-by-keys conn :p ["age < ?" 30])))
    (check "find-by-keys :all with :columns, :order-by desc, :limit, :offset" [{:name "cy"} {:name "bo"}]
           (sql/find-by-keys conn :p :all {:columns [:name] :order-by [[:id :desc]] :limit 2 :offset 1}))
    (check "find-by-keys :offset without :limit" ["cy" "di"]
           (mapv :name (sql/find-by-keys conn :p :all {:order-by [:id] :offset 2})))
    (check "find-by-keys with no match answers []" []
           (sql/find-by-keys conn :p {:name "nobody"}))
    (check "get-by-id answers the row" {:id 2 :name "bo" :age 20 :city "rome"}
           (sql/get-by-id conn :p 2))
    (check "get-by-id answers nil when missing" nil
           (sql/get-by-id conn :p 42))
    (check "get-by-id with a pk name" "di"
           (:name (sql/get-by-id conn :p "oslo" :city {})))
    (.close conn))

  (println "datasources")
  (let [d (nj/get-datasource {:dbtype "sqlite" :dbname ":memory:"})]
    (check "get-datasource answers instance? DataSource" true
           (instance? javax.sql.DataSource d))
    (let [c (nj/get-connection d)]
      (check "a datasource opens connections" [{:one 1}]
             (nj/execute! c ["select 1 as one"]))
      (.close c))
    ;; :memory: gives every connection its own db, so per-op ownership is
    ;; observed through a file-backed store
    (let [f (str "/tmp/njdbc-test-" (System/currentTimeMillis) ".db")
          fd (nj/get-datasource {:dbtype "sqlite" :dbname f})]
      (nj/execute! fd "create table d (x integer)")
      (nj/execute! fd ["insert into d (x) values (?)" 5])
      (check "ops on a datasource open and close per call" [{:x 5}]
             (nj/execute! fd ["select * from d"]))
      (nj/with-transaction [tx fd]
        (nj/execute! tx ["insert into d (x) values (?)" 6]))
      (check "with-transaction on a datasource commits and closes" 2
             (count (nj/execute! fd ["select * from d"])))
      (check "sql/query accepts a datasource" [{:x 5} {:x 6}]
             (sql/query fd ["select * from d order by x"]))
      (nj/execute! fd "create table di (id integer primary key, x integer)")
      (check "sql/insert! accepts a datasource" {:id 1 :x 7}
             (sql/insert! fd :di {:x 7}))
      (check "sql/update! accepts a datasource" {:next.jdbc/update-count 1}
             (sql/update! fd :di {:x 8} {:id 1}))
      (check "sql/get-by-id accepts a datasource" {:id 1 :x 8}
             (sql/get-by-id fd :di 1))
      (check "sql/find-by-keys accepts a datasource" [{:id 1 :x 8}]
             (sql/find-by-keys fd :di {:x 8}))
      (check "sql/delete! accepts a datasource and a map where-clause"
             {:next.jdbc/update-count 1}
             (sql/delete! fd :di {:x 8}))
      (check "sql/delete! accepts a [where & params] vector"
             {:next.jdbc/update-count 2}
             (sql/delete! fd :d ["x > ?" 0]))
      (ds/close-datasource fd)
      (check "acquire after close-datasource throws" :closed
             (try (ds/acquire fd) :open (catch Exception _ :closed)))
      (.delete (java.io.File. f))))

  (when-let [pg-uri (System/getenv "JOLT_TEST_PG_URI")]
    (println "next.jdbc PG value model (" pg-uri ")")
    (let [conn (nj/get-connection pg-uri)]
      (nj/execute! conn "drop table if exists nj_vals")
      (nj/execute! conn "create table nj_vals (id serial primary key, u uuid, n numeric(10,2), ts timestamp, tz timestamptz, d date, t time, tags text[], nums int4[])")
      (let [u (random-uuid)]
        (nj/execute! conn ["insert into nj_vals (u, n, ts, tz, d, t, tags, nums) values (?, ?::numeric, ?::timestamp, ?::timestamptz, ?::date, ?::time, ?::text[], ?::int4[])"
                           u "1.50" "2020-01-02 03:04:05.123456" "2020-01-02 03:04:05.123456+00" "2020-01-02" "03:04:05"
                           ["a" "b,c" "d\"e"] [1 2 3]])
        (let [r (nj/execute-one! conn ["select * from nj_vals"])]
          (check "pg uuid column reads as a uuid" true (uuid? (:u r)))
          (check "pg uuid round-trips" u (:u r))
          (check "pg numeric reads as a bigdec" true (decimal? (:n r)))
          (check "pg numeric keeps its value" 1.50M (:n r))
          (check "pg timestamp reads as a LocalDateTime" true
                 (instance? java.time.LocalDateTime (:ts r)))
          (check "pg timestamp value" "2020-01-02T03:04:05.123456"
                 (str (:ts r)))
          (check "pg timestamptz reads as an OffsetDateTime" true
                 (instance? java.time.OffsetDateTime (:tz r)))
          (check "pg date reads as a LocalDate" "2020-01-02" (str (:d r)))
          (check "pg time reads as a LocalTime" "03:04:05" (str (:t r)))
          (check "pg text[] reads as a vector (quotes, commas, escapes)"
                 ["a" "b,c" "d\"e"] (:tags r))
          (check "pg int4[] reads as longs" [1 2 3] (:nums r))))
      (check "pg empty array reads as []" []
             (:c (nj/execute-one! conn ["select '{}'::text[] as c"])))
      (check "pg array NULL element reads as nil" ["a" nil "b"]
             (:c (nj/execute-one! conn ["select array['a', null, 'b']::text[] as c"])))
      (check "pg uuid param binds without a cast" 1
             (let [u2 (random-uuid)]
               (nj/execute! conn ["insert into nj_vals (u) values (?)" u2])
               (count (nj/execute! conn ["select id from nj_vals where u = ?" u2]))))
      (check "pg vector param binds as an array literal" ["x" "y z"]
             (:c (nj/execute-one! conn ["select ?::text[] as c" ["x" "y z"]])))
      (check "pg execute-batch! over param groups" [1 1]
             (nj/execute-batch! conn "insert into nj_vals (n) values (?::numeric)" [["1"] ["2"]] {}))
      (nj/execute! conn "drop table if exists nj_p")
      (nj/execute! conn "create table nj_p (id serial primary key, name text)")
      (doseq [n ["a" "b" "c"]] (sql/insert! conn :nj_p {:name n}))
      (check "pg sql/update! answers the update count" {:next.jdbc/update-count 1}
             (sql/update! conn :nj_p {:name "bb"} {:name "b"}))
      (check "pg sql/get-by-id" "bb" (:name (sql/get-by-id conn :nj_p 2)))
      (check "pg find-by-keys :offset without :limit" ["bb" "c"]
             (mapv :name (sql/find-by-keys conn :nj_p :all {:order-by [:id] :offset 1})))
      (check "pg find-by-keys :limit and :offset" ["bb"]
             (mapv :name (sql/find-by-keys conn :nj_p :all {:order-by [:id] :limit 1 :offset 1})))
      (nj/execute! conn "drop table nj_p")
      (nj/execute! conn "drop table nj_vals")
      (.close conn))))
