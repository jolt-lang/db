(ns jdbc-conformance
  "Runs clojure.jdbc's OWN test suite against this library.

  db.jdbc-shim stands in for java.sql so the published clojure.jdbc runs
  unmodified. The only way to know it still does is to run the tests upstream
  ships, from upstream's tree, at the sha deps.edn pins — not a copy of them
  kept here, which drifts silently and then agrees with whatever we broke.

  The namespaces are the sqlite-only set upstream nominates for this in its own
  tests-jolt.edn. jdbc.postgres-test is excluded there because it needs a live
  postgres and hikari-cp; db's own suite covers the postgres driver."
  (:require [clojure.test :as t]
            [db.jdbc]
            [jdbc.core-test]
            [jdbc.util-test]
            [jdbc.impl-test]
            [jdbc.meta-test]))

(def ^:private namespaces
  ['jdbc.core-test 'jdbc.util-test 'jdbc.impl-test 'jdbc.meta-test])

(defn -main [& _]
  (let [{:keys [fail error] :as r} (apply t/run-tests namespaces)]
    (println "clojure.jdbc conformance:" (pr-str r))
    (when (pos? (+ (or fail 0) (or error 0)))
      (println "clojure.jdbc's own suite no longer passes against db.jdbc-shim.")
      (System/exit 1))))
