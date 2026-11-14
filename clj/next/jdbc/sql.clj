(ns next.jdbc.sql
  "The next.jdbc.sql friendly functions (query, insert!, update!, delete!,
  find-by-keys, get-by-id), over jdbc.core.
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

(defn update!
  "Set the columns in key-map on rows matching where-params, a column->value
  map or [where-clause & params]. Answers {:next.jdbc/update-count n}."
  ([connectable table key-map where-params] (update! connectable table key-map where-params {}))
  ([connectable table key-map where-params _opts]
   (let [ks (keys key-map)
         [where & params] (where-clause where-params)]
     (njdbc/execute-one! connectable
                         (-> [(str "UPDATE " (name table)
                                   " SET " (str/join ", " (map #(str (name %) " = ?") ks))
                                   (when-not (str/blank? where) (str " WHERE " where)))]
                             (into (map #(get key-map %) ks))
                             (into params))))))

(defn- order-by-sql [order-by]
  (str/join ", " (map (fn [o]
                        (if (sequential? o)
                          (let [[col dir] o] (str (name col) " " (str/upper-case (name dir))))
                          (name o)))
                      order-by)))

(defn find-by-keys
  "Rows of table matching key-map: a column->value map, [where-clause & params],
  or :all. Options: :columns (a seq of column keywords), :order-by (columns or
  [column :asc|:desc] pairs), :limit and :offset."
  ([connectable table key-map] (find-by-keys connectable table key-map {}))
  ([connectable table key-map opts]
   (let [{:keys [columns order-by limit offset]} opts
         [where & params] (when-not (= :all key-map) (where-clause key-map))]
     (njdbc/call-with-connection connectable
      (fn [w]
       (query w
            (into [(str "SELECT " (if (seq columns) (str/join ", " (map name columns)) "*")
                        " FROM " (name table)
                        (when-not (str/blank? where) (str " WHERE " where))
                        (when (seq order-by) (str " ORDER BY " (order-by-sql order-by)))
                        (when limit " LIMIT ?")
                        ;; sqlite only takes OFFSET after a LIMIT; postgres
                        ;; rejects a negative one
                        (when offset
                          (str (when (and (not limit)
                                          (= "SQLite" (.getDatabaseProductName (.getMetaData w))))
                                 " LIMIT -1")
                               " OFFSET ?")))]
                  (concat params (when limit [limit]) (when offset [offset])))
            (dissoc opts :columns :order-by :limit :offset)))))))

(defn get-by-id
  "The row of table whose pk-name column (default :id) equals pk, or nil."
  ([connectable table pk] (get-by-id connectable table pk :id {}))
  ([connectable table pk opts] (get-by-id connectable table pk :id opts))
  ([connectable table pk pk-name opts]
   (first (find-by-keys connectable table {pk-name pk} opts))))
