(ns next.jdbc.sql
  "next.jdbc.sql's friendly functions: query, insert!, insert-multi!, update!,
  delete!, find-by-keys, aggregate-by-keys and get-by-id, with upstream's
  options (see next.jdbc.sql.builder). Every function takes a connectable: an
  open connection, a datasource or a db-spec (opened and closed per call).

  Rows are unqualified lower-cased keyword maps (see next.jdbc), so
  result-set builder-fns are ignored. Generated keys come back through
  RETURNING *, as neither driver has a generated-keys channel: they are the
  whole inserted row, which is also what postgres' own JDBC driver reports."
  (:require [db.jdbc-shim :as shim]
            [jdbc.core :as jc]
            [jdbc.proto :as proto]
            [next.jdbc :as njdbc]
            [next.jdbc.sql.builder :as b]))

(defn- with-dialect
  "Call (f w opts) on a connection for `connectable`, with the database's
  product name in opts for the builders."
  [connectable opts f]
  (njdbc/call-with-connection connectable
    (fn [w]
      (f w (assoc opts ::b/dialect (.getDatabaseProductName (.getMetaData w)))))))

(defn query
  "Run [sql & params] (or a sql string) and return a vector of row maps."
  ([connectable q] (query connectable q {}))
  ([connectable q opts]
   (njdbc/call-with-connection connectable
     (fn [w] (jc/fetch (njdbc/conn-raw w) q (dissoc opts :builder-fn))))))

(defn- returning [[sql & params] opts]
  (if (false? (:return-keys opts))
    (into [sql] params)
    (into [(str sql " RETURNING *")] params)))

(defn insert!
  "Insert one row map into table and return the generated keys (the inserted
  row), or nil when a :suffix such as ON CONFLICT DO NOTHING skipped it.
  {:return-keys false} answers {:next.jdbc/update-count n} instead."
  ([connectable table key-map] (insert! connectable table key-map {}))
  ([connectable table key-map opts]
   (njdbc/execute-one! connectable (returning (b/for-insert table key-map opts) opts) opts)))

(def ^:private max-rows-per-insert
  ;; past a few hundred rows a longer VALUES list buys little and costs parse time
  1000)

(defn- insert-batched
  "insert-multi! :batch. The rows go in as multi-row INSERTs, as many rows per
  statement as the parameter limit allows. Every full-size statement shares
  one SQL string, so those run as one prepared batch; a short tail runs on its
  own. All of it is one transaction (a savepoint inside an open one)."
  [connectable table cols rows opts]
  (njdbc/transact* connectable
    (fn [tx]
      (let [opts (dissoc opts :batch)
            limit (shim/max-params (proto/connection (njdbc/conn-raw tx)))
            per (max 1 (min max-rows-per-insert (quot limit (count cols))))
            chunks (partition-all per rows)
            full (filter #(= per (count %)) chunks)
            tail (remove #(= per (count %)) chunks)
            sql-for (fn [chunk] (returning (b/for-insert-multi table cols chunk opts) opts))]
        (cond-> []
          (seq full) (into (njdbc/execute-batch! tx (first (sql-for (first full)))
                                                 (map #(into [] cat %) full)
                                                 {:return-generated-keys true}))
          (seq tail) (into (njdbc/execute! tx (sql-for (first tail)))))))
    {}))

(defn insert-multi!
  "Insert rows, given a seq of hash maps with the same keys, or a column
  vector and rows of values. Answers the inserted rows. Without :batch this
  is one INSERT with placeholders for every value; with :batch it is split
  into multi-row INSERTs that fit the parameter limit, run as one prepared
  batch in one transaction."
  {:arglists '([connectable table hash-maps]
               [connectable table hash-maps opts]
               [connectable table cols rows]
               [connectable table cols rows opts])}
  ([connectable table hash-maps] (insert-multi! connectable table hash-maps {}))
  ([connectable table hash-maps-or-cols opts-or-rows]
   (if (map? (first hash-maps-or-cols))
     (let [cols (keys (first hash-maps-or-cols))]
       (when-not (apply = (map (comp set keys) hash-maps-or-cols))
         (throw (IllegalArgumentException. "insert-multi! hash maps must all have the same keys")))
       (insert-multi! connectable table cols
                      (map (fn [m] (map #(get m %) cols)) hash-maps-or-cols)
                      opts-or-rows))
     (if (map? opts-or-rows)
       (insert-multi! connectable table hash-maps-or-cols [] opts-or-rows)
       (insert-multi! connectable table hash-maps-or-cols opts-or-rows {}))))
  ([connectable table cols rows opts]
   (if (seq rows)
     (let [opts (assoc opts :return-keys true)]
       (if (:batch opts)
         (insert-batched connectable table cols rows opts)
         (njdbc/execute! connectable (returning (b/for-insert-multi table cols rows opts) opts))))
     [])))

(defn update!
  "Set the columns in key-map on rows matching where-params, a column->value
  map or [where-clause & params]. Answers {:next.jdbc/update-count n}."
  ([connectable table key-map where-params] (update! connectable table key-map where-params {}))
  ([connectable table key-map where-params opts]
   (njdbc/execute-one! connectable (b/for-update table key-map where-params opts) opts)))

(defn delete!
  "Delete rows matching where-params, a column->value map or
  [where-clause & params]. Answers {:next.jdbc/update-count n}."
  ([connectable table where-params] (delete! connectable table where-params {}))
  ([connectable table where-params opts]
   (njdbc/execute-one! connectable (b/for-delete table where-params opts) opts)))

(defn find-by-keys
  "Rows of table matching key-map: a column->value map, [where-clause & params],
  or :all. Options: :columns, :order-by, :suffix, :table-fn / :column-fn, and
  the pagination options :top, :limit, :offset and :fetch."
  ([connectable table key-map] (find-by-keys connectable table key-map {}))
  ([connectable table key-map opts]
   (with-dialect connectable opts
     (fn [w opts] (njdbc/execute! w (b/for-query table key-map opts) opts)))))

(defn aggregate-by-keys
  "The value of a SQL aggregate expression (a string, such as \"count(*)\")
  over the rows find-by-keys would match. Takes find-by-keys' options except
  :columns."
  ([connectable table aggregate key-map] (aggregate-by-keys connectable table aggregate key-map {}))
  ([connectable table aggregate key-map opts]
   (when-not (string? aggregate)
     (throw (IllegalArgumentException. "aggregate-by-keys requires a string aggregate expression")))
   (when (:columns opts)
     (throw (IllegalArgumentException. "aggregate-by-keys does not support the :columns option")))
   (let [total (keyword (str "next_jdbc_aggregate_" (Math/abs (long (hash aggregate)))))]
     (-> (find-by-keys connectable table key-map (assoc opts :columns [[aggregate total]]))
         first
         (get total)))))

(defn get-by-id
  "The row of table whose pk-name column (default :id) equals pk, or nil.
  Takes find-by-keys' options."
  ([connectable table pk] (get-by-id connectable table pk :id {}))
  ([connectable table pk opts] (get-by-id connectable table pk :id opts))
  ([connectable table pk pk-name opts]
   (first (find-by-keys connectable table {pk-name pk} opts))))
