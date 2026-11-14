(ns next.jdbc.sql
  "The next.jdbc.sql friendly functions (query/insert!/delete!), over jdbc.core.
  Every function takes a connectable, as upstream does: an open connection, a
  datasource or a db-spec (opened and closed per call). next.jdbc result-set
  builder-fns are ignored because jdbc.core already returns unqualified,
  lower-cased, keyword-keyed row maps."
  (:require [clojure.string :as str]
            [jdbc.core :as jc]
            [next.jdbc :as njdbc]))

(defn- with-raw
  "Call f with the raw jdbc.core connection behind `connectable`."
  [connectable f]
  (njdbc/call-with-connection connectable (fn [w] (f (njdbc/conn-raw w)))))

(defn query
  "Run [sql & params] (or a sql string) and return a vector of row maps."
  ([connectable q] (query connectable q {}))
  ([connectable q opts]
   (with-raw connectable #(jc/fetch % q (dissoc opts :builder-fn)))))

(defn insert!
  "Insert one row map into table and return the generated keys, as next.jdbc
  does. Neither driver has a generated-keys channel, so they come back through
  RETURNING * and the map is the whole inserted row, which is also what
  postgres' own JDBC driver reports. {:return-keys false} answers
  {:next.jdbc/update-count n} instead, as next.jdbc's execute-one! does."
  ([connectable table key-map] (insert! connectable table key-map {}))
  ([connectable table key-map opts]
   (with-raw connectable
     (fn [raw]
       (if (false? (:return-keys opts))
         {:next.jdbc/update-count (first (jc/insert! raw table key-map {}))}
         (first (jc/insert! raw table key-map {:returning true})))))))

(defn- where-clause
  "A next.jdbc where: a column->value map (nil matches IS NULL) or
  [where-sql & params]."
  [where]
  (if (map? where)
    (let [ks (keys where)]
      (into [(str/join " AND " (map (fn [k] (str (name k) (if (nil? (get where k)) " IS NULL" " = ?"))) ks))]
            (remove nil? (map #(get where %) ks))))
    where))

(defn delete!
  "Delete rows matching where-params, a column->value map or
  [where-clause & params] such as [\"id=?\" 7]. Answers
  {:next.jdbc/update-count n}, as upstream does."
  ([connectable table where-params] (delete! connectable table where-params {}))
  ([connectable table where-params _opts]
   (let [[where & params] (where-clause where-params)]
     (njdbc/execute-one! connectable
                         (into [(str "DELETE FROM " (name table)
                                     (when-not (str/blank? where) (str " WHERE " where)))]
                               params)))))
