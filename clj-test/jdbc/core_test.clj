(ns jdbc.core-test
  (:require [jdbc.core :as jdbc]))

(def failures (atom 0))

(defn check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "— expected" (pr-str expected) "got" (pr-str actual)))))

(defn -main [& _]
  (println "jdbc.core over sqlite (:memory:)")
  (with-open [conn (jdbc/connection "sqlite::memory:")]
    (check "execute! ddl" 0
           (jdbc/execute! conn "create table person (id integer primary key, name text, zip integer)"))
    (check "insert! returns the id" 1 (jdbc/insert! conn :person {:name "ada" :zip 94546}))
    (check "insert-multi! ids" [2 3]
           (jdbc/insert-multi! conn :person [{:name "grace" :zip 94546}
                                             {:name "alan" :zip 10001}]))
    (check "fetch sqlvec with params" [{:id 1 :name "ada" :zip 94546}]
           (jdbc/fetch conn ["select * from person where name = ?" "ada"]))
    (check "fetch-one" {:id 3 :name "alan" :zip 10001}
           (jdbc/fetch-one conn ["select * from person where zip = ?" 10001]))
    (check "fetch plain string" 3
           (count (jdbc/fetch conn "select * from person")))
    (check "SQL errors catch java.sql.SQLException" :caught
           (try
             (jdbc/fetch conn "select * from missing_table")
             (catch java.sql.SQLException _ :caught)))
    (check "SQL errors still catch Exception" :caught
           (try
             (jdbc/fetch conn "select * from missing_table")
             (catch Exception _ :caught)))
    (jdbc/execute! conn "create table payload (id integer primary key, content blob not null)")
    (doseq [[label payload] [["embedded NULs" (byte-array [65 0 66 0 67])]
                             ["non-UTF-8 bytes" (byte-array [-1 -2])]
                             ["empty payload" (byte-array [])]]]
      (let [id (jdbc/insert! conn :payload {:content payload})
            actual (:content
                    (jdbc/fetch-one conn
                                    ["select content from payload where id = ?" id]))]
        (check (str "BLOB " label " returns bytes") true (bytes? actual))
        (check (str "BLOB " label " round-trips") (vec payload) (vec actual))))
    (check "update! rows affected" 2
           (jdbc/update! conn :person {:zip 94540} ["zip = ?" 94546]))
    (check "update applied" 2
           (count (jdbc/fetch conn ["select * from person where zip = ?" 94540])))
    (check "delete! rows affected" 1
           (jdbc/delete! conn :person ["name = ?" "alan"]))

    (println "transactions")
    (jdbc/atomic conn
      (jdbc/insert! conn :person {:name "tx" :zip 1}))
    (check "atomic commits" 1
           (count (jdbc/fetch conn ["select * from person where name = ?" "tx"])))
    (check "atomic rolls back on throw" :threw
           (try (jdbc/atomic conn
                  (jdbc/insert! conn :person {:name "boom" :zip 2})
                  (throw (ex-info "no" {})))
                (catch Throwable _ :threw)))
    (check "rollback discarded the insert" 0
           (count (jdbc/fetch conn ["select * from person where name = ?" "boom"])))
    (jdbc/atomic conn
      (jdbc/insert! conn :person {:name "outer" :zip 3})
      (try (jdbc/atomic conn
             (jdbc/insert! conn :person {:name "inner" :zip 4})
             (throw (ex-info "inner-only" {})))
           (catch Throwable _ nil)))
    (check "nested savepoint: outer survives" 1
           (count (jdbc/fetch conn ["select * from person where name = ?" "outer"])))
    (check "nested savepoint: inner rolled back" 0
           (count (jdbc/fetch conn ["select * from person where name = ?" "inner"])))
    (jdbc/atomic conn
      (jdbc/insert! conn :person {:name "marked" :zip 5})
      (jdbc/set-rollback! conn))
    (check "set-rollback! discards the block" 0
           (count (jdbc/fetch conn ["select * from person where name = ?" "marked"]))))

  (println "dbspec parsing")
  (check "map spec works" 1
         (with-open [c (jdbc/connection {:vendor "sqlite" :name ":memory:"})]
           (jdbc/execute! c "create table t (x integer)")
           (jdbc/insert! c :t {:x 7})))

  (when-let [pg-uri (System/getenv "JOLT_TEST_PG_URI")]
    (println "jdbc.core over postgres (" pg-uri ")")
    (with-open [conn (jdbc/connection pg-uri)]
      (jdbc/execute! conn "drop table if exists jolt_person")
      (jdbc/execute! conn "create table jolt_person (id serial primary key, name text, zip integer)")
      (check "pg insert! returns the id" 1 (jdbc/insert! conn :jolt_person {:name "ada" :zip 94546}))
      (check "pg fetch with ? params" [{:id 1 :name "ada" :zip 94546}]
             (jdbc/fetch conn ["select * from jolt_person where name = ?" "ada"]))
      (jdbc/insert! conn :jolt_person {:name "grace" :zip 94546})
      (jdbc/update! conn :jolt_person {:zip 94540} ["zip = ?" 94546])
      (check "pg update applied" 2
             (count (jdbc/fetch conn ["select * from jolt_person where zip = ?" 94540])))
      (jdbc/atomic conn
        (jdbc/insert! conn :jolt_person {:name "tx" :zip 1}))
      (check "pg atomic commits" 1
             (count (jdbc/fetch conn ["select * from jolt_person where name = ?" "tx"])))
      (check "pg atomic rolls back" :threw
             (try (jdbc/atomic conn
                    (jdbc/insert! conn :jolt_person {:name "boom" :zip 2})
                    (throw (ex-info "no" {})))
                  (catch Throwable _ :threw)))
      (check "pg rollback discarded" 0
             (count (jdbc/fetch conn ["select * from jolt_person where name = ?" "boom"])))
      (check "pg SQL errors catch java.sql.SQLException" :caught
             (try
               (jdbc/fetch conn "select * from jolt_missing_table")
               (catch java.sql.SQLException _ :caught)))
      (check "pg SQL errors still catch Exception" :caught
             (try
               (jdbc/fetch conn "select * from jolt_missing_table")
               (catch Exception _ :caught)))
      (jdbc/execute! conn "drop table if exists jolt_payload")
      (jdbc/execute! conn "create table jolt_payload (id serial primary key, content bytea not null)")
      ;; bytea reads back as text, in whichever format bytea_output names, so run
      ;; the round-trip under both. hex is the default and the only one a stock
      ;; server exercises; escape is what older servers and anyone who has set it
      ;; back will send, and its decoder is the easier of the two to get wrong.
      (doseq [mode ["hex" "escape"]]
        (jdbc/execute! conn (str "set bytea_output = '" mode "'"))
        (doseq [[label payload] [["embedded NULs" (byte-array [65 0 66 0 67])]
                                 ["non-UTF-8 bytes" (byte-array [-1 -2])]
                                 ["backslashes and octal digits" (byte-array [92 92 48 48 48 92])]
                                 ["every byte value" (byte-array (mapv (fn [i] (if (> i 127) (- i 256) i))
                                                                       (range 256)))]
                                 ["empty payload" (byte-array [])]]]
          (let [id (jdbc/insert! conn :jolt_payload {:content payload})
                actual (:content
                        (jdbc/fetch-one conn
                                        ["select content from jolt_payload where id = ?" id]))]
            (check (str "pg bytea/" mode " " label " returns bytes") true (bytes? actual))
            (check (str "pg bytea/" mode " " label " round-trips") (vec payload) (vec actual)))))
      (jdbc/execute! conn "reset bytea_output")
      ;; A bytea parameter carries its own type, so it does not depend on there
      ;; being a bytea column in the statement for postgres to infer from. Without
      ;; that, `select ?` infers text and the parameter comes back as the literal
      ;; string the driver sent rather than as bytes.
      (let [payload (byte-array [65 0 -1 92 66])]
        (check "pg bytea param needs no column to infer its type from"
               (vec payload)
               (vec (:c (jdbc/fetch-one conn ["select ? as c" payload]))))
        (check "pg bytea param is bytes with nothing to infer from"
               true
               (bytes? (:c (jdbc/fetch-one conn ["select ? as c" payload]))))
        (check "pg bytea param compares against a stored value" 1
               (do (jdbc/execute! conn "delete from jolt_payload")
                   (jdbc/insert! conn :jolt_payload {:content payload})
                   (count (jdbc/fetch conn ["select id from jolt_payload where content = ?" payload]))))
        (check "pg bytea param round-trips through a cast" (vec payload)
               (vec (:c (jdbc/fetch-one conn ["select cast(? as bytea) as c" payload]))))
        ;; the per-parameter type / length / format arrays have to stay aligned
        ;; with the values array, so mix a bytea in among text parameters
        (let [r (jdbc/fetch-one conn ["select ? as a, ? as b, ? as d" "before" payload "after"])]
          (check "pg mixed text and bytea params keep their positions"
                 ["before" (vec payload) "after"]
                 [(:a r) (vec (:b r)) (:d r)]))
        (check "pg empty bytea param survives with nothing to infer from" []
               (vec (:c (jdbc/fetch-one conn ["select ? as c" (byte-array [])]))))
        (check "pg nil param is still SQL NULL" nil
               (:c (jdbc/fetch-one conn ["select ? as c" nil])))
        (check "pg text param is unaffected" "plain"
               (:c (jdbc/fetch-one conn ["select ? as c" "plain"])))
        ;; a size the old hex literal would have sent as twice as many bytes, and
        ;; enough of them to catch a binary bind that only walks part of the array
        (let [big (byte-array (mapv (fn [i] (let [v (mod (* i 7) 256)]
                                              (if (> v 127) (- v 256) v)))
                                    (range 65536)))]
          (check "pg 64KB bytea param round-trips" (vec big)
                 (vec (:c (jdbc/fetch-one conn ["select ? as c" big]))))))
      (jdbc/execute! conn "drop table jolt_payload")
      (jdbc/execute! conn "drop table jolt_person")))

  (if (pos? @failures)
    (throw (ex-info "test failures" {:n @failures}))
    (println "all checks passed")))
