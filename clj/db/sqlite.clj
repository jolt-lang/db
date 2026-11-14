(ns db.sqlite
  "SQLite driver for jolt, binding the system libsqlite3 through jolt.ffi. Exposes
  the surface jdbc.core needs: open / close / query (rows as keyword-keyed maps) /
  changes / last-insert-rowid. No jolt built-in — the binding lives here."
  (:require [jolt.ffi :as ffi]
            [clojure.string :as str]))

;; libsqlite3 is declared in deps.edn (:jolt/native) and loaded by jolt before
;; this namespace is required, so the bindings below resolve.

;; --- bindings ----------------------------------------------------------------
(ffi/defcfn sqlite3-open          "sqlite3_open"          [:string :pointer] :int)
(ffi/defcfn sqlite3-close         "sqlite3_close"         [:pointer] :int)
(ffi/defcfn sqlite3-errmsg        "sqlite3_errmsg"        [:pointer] :string)
(ffi/defcfn sqlite3-prepare       "sqlite3_prepare_v2"    [:pointer :string :int :pointer :pointer] :int)
(ffi/defcfn sqlite3-step          "sqlite3_step"          [:pointer] :int)
(ffi/defcfn sqlite3-finalize      "sqlite3_finalize"      [:pointer] :int)
(ffi/defcfn sqlite3-column-count  "sqlite3_column_count"  [:pointer] :int)
(ffi/defcfn sqlite3-column-name   "sqlite3_column_name"   [:pointer :int] :string)
(ffi/defcfn sqlite3-column-type   "sqlite3_column_type"   [:pointer :int] :int)
(ffi/defcfn sqlite3-column-text   "sqlite3_column_text"   [:pointer :int] :string)
(ffi/defcfn sqlite3-column-blob   "sqlite3_column_blob"   [:pointer :int] :pointer)
(ffi/defcfn sqlite3-column-bytes  "sqlite3_column_bytes"  [:pointer :int] :int)
(ffi/defcfn sqlite3-column-int64  "sqlite3_column_int64"  [:pointer :int] :int64)
(ffi/defcfn sqlite3-column-double "sqlite3_column_double" [:pointer :int] :double)
(ffi/defcfn sqlite3-bind-text     "sqlite3_bind_text"     [:pointer :int :string :int :iptr] :int)
(ffi/defcfn sqlite3-bind-blob     "sqlite3_bind_blob"     [:pointer :int :pointer :int :iptr] :int)
(ffi/defcfn sqlite3-bind-int64    "sqlite3_bind_int64"    [:pointer :int :int64] :int)
(ffi/defcfn sqlite3-bind-double   "sqlite3_bind_double"   [:pointer :int :double] :int)
(ffi/defcfn sqlite3-bind-null     "sqlite3_bind_null"     [:pointer :int] :int)
(ffi/defcfn sqlite3-changes       "sqlite3_changes"       [:pointer] :int)
(ffi/defcfn sqlite3-total-changes "sqlite3_total_changes" [:pointer] :int)
(ffi/defcfn sqlite3-last-rowid    "sqlite3_last_insert_rowid" [:pointer] :int64)
(ffi/defcfn sqlite3-reset         "sqlite3_reset"         [:pointer] :int)
(ffi/defcfn sqlite3-clear-bindings "sqlite3_clear_bindings" [:pointer] :int)
(ffi/defcfn sqlite3-get-autocommit "sqlite3_get_autocommit" [:pointer] :int)
(ffi/defcfn sqlite3-limit         "sqlite3_limit"         [:pointer :int :int] :int)

(def ^:private SQLITE-OK 0)
(def ^:private SQLITE-ROW 100)
(def ^:private SQLITE-DONE 101)
(def ^:private SQLITE-TRANSIENT -1)        ; tell sqlite to copy the bound data

;; column storage classes (sqlite3_column_type)
(def ^:private TY-INT 1) (def ^:private TY-FLOAT 2) (def ^:private TY-BLOB 4) (def ^:private TY-NULL 5)

;; --- connection --------------------------------------------------------------
(defn open
  "Open `path` (a file or \":memory:\"). Returns the sqlite3* pointer."
  [path]
  (let [pp (ffi/alloc (ffi/sizeof :pointer))]
    (try
      (let [rc (sqlite3-open path pp)
            db (ffi/read pp :pointer)]
        (when-not (= rc SQLITE-OK)
          (throw (ex-info (str "sqlite open failed: " path)
                          {:rc rc :jdbc/sql-error true})))
        db)
      (finally (ffi/free pp)))))

(defn close [db] (sqlite3-close db) nil)

(defn- bind-params! [stmt params]
  (loop [i 1 ps (seq params)]
    (when ps
      (let [v (first ps)]
        (cond
          (nil? v)                       (sqlite3-bind-null stmt i)
          (bytes? v)                     (let [n (alength v)
                                               ptr (ffi/alloc (max 1 n))]
                                           (try
                                             (ffi/write-array ptr v)
                                             (sqlite3-bind-blob stmt i ptr n SQLITE-TRANSIENT)
                                             (finally
                                               (ffi/free ptr))))
          (and (integer? v) (int? v))    (sqlite3-bind-int64 stmt i v)
          (number? v)                    (sqlite3-bind-double stmt i (double v))
          (string? v)                    (sqlite3-bind-text stmt i v -1 SQLITE-TRANSIENT)
          :else                          (sqlite3-bind-text stmt i (str v) -1 SQLITE-TRANSIENT)))
      (recur (inc i) (next ps)))))

(defn- read-value [stmt i]
  (let [ty (sqlite3-column-type stmt i)]
    (cond
      (= ty TY-INT)   (sqlite3-column-int64 stmt i)
      (= ty TY-FLOAT) (sqlite3-column-double stmt i)
      (= ty TY-BLOB)  (let [n (sqlite3-column-bytes stmt i)]
                        (ffi/read-array (sqlite3-column-blob stmt i) n))
      (= ty TY-NULL)  nil
      :else           (sqlite3-column-text stmt i))))

(defn- read-values [stmt n]
  (loop [i 0 acc (transient [])]
    (if (= i n) (persistent! acc) (recur (inc i) (conj! acc (read-value stmt i))))))

(defn- prepare!
  "Compile `sql` into a sqlite3_stmt*, which the caller finalizes."
  [db sql]
  (let [pp (ffi/alloc (ffi/sizeof :pointer))
        rc (sqlite3-prepare db sql -1 pp ffi/null)
        stmt (ffi/read pp :pointer)]
    (ffi/free pp)
    (when-not (= rc SQLITE-OK)
      (throw (ex-info (str "sqlite prepare failed: " (sqlite3-errmsg db) " — " sql)
                      {:jdbc/sql-error true})))
    stmt))

(defn- step-all
  "Step a bound statement to completion; the rows it produced."
  [db stmt ncol]
  (loop [rows (transient [])]
    (let [r (sqlite3-step stmt)]
      (cond
        (= r SQLITE-ROW)  (recur (conj! rows (read-values stmt ncol)))
        (= r SQLITE-DONE) (persistent! rows)
        :else (throw (ex-info (str "sqlite step failed: " (sqlite3-errmsg db))
                              {:rc r :jdbc/sql-error true}))))))

(defn- labels-of [stmt]
  (let [ncol (sqlite3-column-count stmt)]
    (mapv (fn [i] (sqlite3-column-name stmt i)) (range ncol))))

(defn query-raw
  "Run `sql` with `params` (a seq); return {:labels [col-name ...] :rows [[v ...]]}.
  Column order is preserved, which a JDBC-shaped caller needs to read a row by
  index. `query` is this with the rows turned into maps."
  [db sql params]
  (let [stmt (prepare! db sql)]
    (try
      (bind-params! stmt params)
      (let [labels (labels-of stmt)]
        {:labels labels :rows (step-all db stmt (count labels))})
      (finally (sqlite3-finalize stmt)))))

(defn execute-batch
  "Run `sql` once per parameter group in `groups`, compiling it once and
  rebinding it for each. Answers a vector of {:labels :rows :count}, one per
  group, where :count is the rows that group changed.

  Outside a transaction the batch runs inside one, so it lands whole or not at
  all, and the database file is synced once instead of once per row. Inside a
  transaction it simply joins it. The test is sqlite's own autocommit state,
  so a transaction opened with a raw BEGIN counts too."
  [db sql groups]
  (if (empty? groups)
    []
    (let [own-tx? (= 1 (sqlite3-get-autocommit db))]
      (when own-tx? (query-raw db "BEGIN" []))
      (try
        (let [out (let [stmt (prepare! db sql)]
                    (try
                      (let [labels (labels-of stmt)
                            ncol (count labels)]
                        (loop [gs (seq groups) out (transient [])]
                          (if-not gs
                            (persistent! out)
                            (let [before (sqlite3-total-changes db)]
                              (sqlite3-reset stmt)
                              (sqlite3-clear-bindings stmt)
                              (bind-params! stmt (first gs))
                              (let [rows (step-all db stmt ncol)]
                                (recur (next gs)
                                       (conj! out {:labels labels :rows rows
                                                   :count (- (sqlite3-total-changes db) before)})))))))
                      (finally (sqlite3-finalize stmt))))]
          (when own-tx? (query-raw db "COMMIT" []))
          out)
        (catch Exception e
          (when (and own-tx? (zero? (sqlite3-get-autocommit db)))
            (query-raw db "ROLLBACK" []))
          (throw e))))))

(def ^:private LIMIT-VARIABLE-NUMBER 9)

(defn max-params
  "The most ? parameters one statement may carry on this connection (999
  before sqlite 3.32, 32766 since, or whatever the build set)."
  [db]
  (sqlite3-limit db LIMIT-VARIABLE-NUMBER -1))

(defn query
  "Run `sql` with `params` (a seq); return a vector of keyword-keyed row maps
  (empty for a non-SELECT)."
  [db sql params]
  (let [{:keys [labels rows]} (query-raw db sql params)
        ks (mapv keyword labels)]
    (mapv (fn [vs] (zipmap ks vs)) rows)))

(defn changes [db] (sqlite3-changes db))
;; changes() reports the LAST row-changing statement, so it reads stale after
;; DDL; a before/after delta of total-changes answers "what did THIS statement
;; change" for any statement.
(defn total-changes [db] (sqlite3-total-changes db))
(defn last-insert-rowid [db] (sqlite3-last-rowid db))
