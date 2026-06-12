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

  (when-let [pg-uri (janet.os/getenv "JOLT_TEST_PG_URI")]
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
      (jdbc/execute! conn "drop table jolt_person")))

  (if (pos? @failures)
    (do (println @failures "failing check(s)") (janet.os/exit 1))
    (println "all checks passed")))
