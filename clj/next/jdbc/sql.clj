(ns next.jdbc.sql
  "The next.jdbc.sql friendly functions migratus uses (query/insert!/delete!),
  over jdbc.core. Connections are unwrapped to the raw jdbc.core conn; next.jdbc
  result-set builder-fns are ignored because jdbc.core already returns
  unqualified, lower-cased, keyword-keyed row maps."
  (:require [jdbc.core :as jc]
            [next.jdbc :as njdbc]))

(defn query
  "Run [sql & params] (or a sql string) and return a vector of row maps."
  ([conn q] (query conn q {}))
  ([conn q opts] (jc/fetch (njdbc/conn-raw conn) q (dissoc opts :builder-fn))))

(defn insert!
  "Insert one row map into table and return the generated keys, as next.jdbc
  does. Neither driver has a generated-keys channel, so they come back through
  RETURNING * and the map is the whole inserted row, which is also what
  postgres' own JDBC driver reports. {:return-keys false} answers
  {:next.jdbc/update-count n} instead, as next.jdbc's execute-one! does."
  ([conn table key-map] (insert! conn table key-map {}))
  ([conn table key-map opts]
   (let [raw (njdbc/conn-raw conn)]
     (if (false? (:return-keys opts))
       {:next.jdbc/update-count (first (jc/insert! raw table key-map {}))}
       (first (jc/insert! raw table key-map {:returning true}))))))

(defn delete!
  "Delete rows. where-params is [where-clause & params], e.g. [\"id=?\" 7]."
  ([conn table where-params] (delete! conn table where-params {}))
  ([conn table where-params _opts] (jc/delete! (njdbc/conn-raw conn) table where-params {})))
