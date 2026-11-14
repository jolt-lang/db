(ns next.jdbc.sql.builder
  "SQL string builders behind next.jdbc.sql, with upstream's names and options:
  :table-fn / :column-fn, :columns (keywords, [col alias] or [expr alias]),
  :order-by, :suffix, and the pagination options :top, :limit, :offset and
  :fetch.

  Upstream emits each pagination option in its own dialect (SQL Server TOP,
  MySQL LIMIT/OFFSET, ANSI OFFSET/FETCH). Neither SQLite nor PostgreSQL takes
  TOP, and SQLite takes no OFFSET without a LIMIT nor any FETCH, so here every
  combination is written as LIMIT/OFFSET, which both understand. The one
  difference between them, an OFFSET with no row limit, is settled by the
  ::dialect option (a database product name); next.jdbc.sql supplies it from
  the connection."
  (:require [clojure.string :as str]))

(def ^:private suspicious #";")

(defn- safe-name
  "name, refusing a table or column name with a ; in it."
  [k]
  (let [entity (name k)]
    (when (re-find suspicious entity)
      (throw (ex-info (str "suspicious character found in entity: " entity)
                      {:disallowed suspicious})))
    entity))

(defn- validate [ok? msg]
  (when-not ok? (throw (IllegalArgumentException. ^String msg))))

(defn- suffix [opts]
  (when-let [s (:suffix opts)] (str " " s)))

(defn as-?
  "A ? placeholder for each column of a key-map or column vector."
  [key-map _opts]
  (str/join ", " (repeat (count key-map) "?")))

(defn as-cols
  "Format column names: a keyword, [col alias], or [\"expr\" alias].
  :column-fn applies to names and aliases, not to expressions."
  [cols opts]
  (let [col-fn (:column-fn opts identity)]
    (str/join ", " (map (fn [raw]
                          (if (vector? raw)
                            (let [[c alias] raw]
                              (str (if (keyword? c) (col-fn (safe-name c)) c)
                                   " AS " (col-fn (safe-name alias))))
                            (col-fn (safe-name raw))))
                        cols))))

(defn as-keys
  "The column names of a key-map."
  [key-map opts]
  (as-cols (keys key-map) opts))

(defn by-keys
  "[clause & params] for a key-map, clause being :set or :where. In a where,
  nil matches IS NULL."
  [key-map clause opts]
  (let [col-fn (:column-fn opts identity)
        [conds params] (reduce-kv (fn [[conds params] k v]
                                    (let [e (col-fn (safe-name k))]
                                      (if (and (= :where clause) (nil? v))
                                        [(conj conds (str e " IS NULL")) params]
                                        [(conj conds (str e " = ?")) (conj params v)])))
                                  [[] []]
                                  key-map)]
    (validate (seq conds) "key-map may not be empty")
    (into [(str (str/upper-case (name clause)) " "
                (str/join (if (= :where clause) " AND " ", ") conds))]
          params)))

(defn- where-of
  "[\"WHERE ...\" & params] (or [nil] for no clause) for a where map,
  [where-sql & params], or :all."
  [where-params opts]
  (cond
    (= :all where-params) [nil]
    (map? where-params) (by-keys where-params :where opts)
    (str/blank? (first where-params)) (into [nil] (rest where-params))
    :else (into [(str "WHERE " (first where-params))] (rest where-params))))

(defn for-order-col
  "The ORDER BY sub-clause for a column or a [column :asc|:desc] pair."
  [col opts]
  (let [col-fn (:column-fn opts identity)]
    (cond
      (keyword? col) (col-fn (safe-name col))
      (and (vector? col) (= 2 (count col)) (keyword? (first col)))
      (str (col-fn (safe-name (first col))) " "
           (or (get {:asc "ASC" :desc "DESC"} (second col))
               (throw (IllegalArgumentException.
                       (str ":order-by " col " expected :asc or :desc")))))
      :else (throw (IllegalArgumentException.
                    (str ":order-by expected keyword or keyword pair, found: " col))))))

(defn for-order
  "An ORDER BY clause for an :order-by vector."
  [order-by opts]
  (validate (vector? order-by) ":order-by must be a vector")
  (validate (seq order-by) ":order-by may not be empty")
  (str "ORDER BY " (str/join ", " (map #(for-order-col % opts) order-by))))

(defn- pagination
  "[sql-fragment & params] for :top / :limit / :fetch (all a row limit, at
  most one of them) and :offset."
  [{:keys [top limit fetch offset] ::keys [dialect]}]
  (let [limits (remove nil? [top limit fetch])]
    (validate (<= (count limits) 1) "only one of :top, :limit and :fetch may be given")
    (let [n (first limits)]
      (cond
        (and n offset) [" LIMIT ? OFFSET ?" n offset]
        n [" LIMIT ?" n]
        offset [(if (= "SQLite" dialect) " LIMIT -1 OFFSET ?" " OFFSET ?") offset]
        :else [nil]))))

(defn for-query
  "[\"SELECT ...\" & params] for a table and a where map, [where-sql & params]
  or :all."
  [table where-params opts]
  (let [table-fn (:table-fn opts identity)
        [where & where-ps] (where-of where-params opts)
        [page & page-ps] (pagination opts)]
    (into [(str "SELECT "
                (if-let [cols (seq (:columns opts))] (as-cols cols opts) "*")
                " FROM " (table-fn (safe-name table))
                (when where (str " " where))
                (when-let [order-by (:order-by opts)] (str " " (for-order order-by opts)))
                page
                (suffix opts))]
          (concat where-ps page-ps))))

(defn for-insert
  "[\"INSERT ...\" & params] for one row map."
  [table key-map opts]
  (validate (seq key-map) "key-map may not be empty")
  (into [(str "INSERT INTO " ((:table-fn opts identity) (safe-name table))
              " (" (as-keys key-map opts) ")"
              " VALUES (" (as-? key-map opts) ")"
              (suffix opts))]
        (vals key-map)))

(defn for-insert-multi
  "[\"INSERT ...\" & params] for a column vector and rows of values. With
  :batch the statement has one row of placeholders and each row follows as
  its own parameter group."
  [table cols rows opts]
  (validate (apply = (count cols) (map count rows))
            "column counts are not consistent across cols and rows")
  (validate (seq cols) "cols may not be empty")
  (validate (seq rows) "rows may not be empty")
  (let [batch? (:batch opts)
        places (str "(" (as-? (first rows) opts) ")")]
    (into [(str "INSERT INTO " ((:table-fn opts identity) (safe-name table))
                " (" (as-cols cols opts) ")"
                " VALUES "
                (if batch? places (str/join ", " (repeat (count rows) places)))
                (suffix opts))]
          (if batch? (map vec) cat)
          rows)))

(defn for-update
  "[\"UPDATE ...\" & params] setting key-map on rows matching where-params."
  [table key-map where-params opts]
  (let [[set-sql & set-ps] (by-keys key-map :set opts)
        [where & where-ps] (where-of where-params opts)]
    (-> [(str "UPDATE " ((:table-fn opts identity) (safe-name table))
              " " set-sql
              (when where (str " " where))
              (suffix opts))]
        (into set-ps)
        (into where-ps))))

(defn for-delete
  "[\"DELETE ...\" & params] for rows matching where-params."
  [table where-params opts]
  (let [[where & where-ps] (where-of where-params opts)]
    (into [(str "DELETE FROM " ((:table-fn opts identity) (safe-name table))
                (when where (str " " where))
                (suffix opts))]
          where-ps)))
