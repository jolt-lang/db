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
(ffi/defcfn sqlite3-column-int64  "sqlite3_column_int64"  [:pointer :int] :int64)
(ffi/defcfn sqlite3-column-double "sqlite3_column_double" [:pointer :int] :double)
(ffi/defcfn sqlite3-bind-text     "sqlite3_bind_text"     [:pointer :int :string :int :iptr] :int)
(ffi/defcfn sqlite3-bind-int64    "sqlite3_bind_int64"    [:pointer :int :int64] :int)
(ffi/defcfn sqlite3-bind-double   "sqlite3_bind_double"   [:pointer :int :double] :int)
(ffi/defcfn sqlite3-bind-null     "sqlite3_bind_null"     [:pointer :int] :int)
(ffi/defcfn sqlite3-changes       "sqlite3_changes"       [:pointer] :int)
(ffi/defcfn sqlite3-last-rowid    "sqlite3_last_insert_rowid" [:pointer] :int64)

(def ^:private SQLITE-OK 0)
(def ^:private SQLITE-ROW 100)
(def ^:private SQLITE-DONE 101)
(def ^:private SQLITE-TRANSIENT -1)        ; tell sqlite to copy the bound text

;; column storage classes (sqlite3_column_type)
(def ^:private TY-INT 1) (def ^:private TY-FLOAT 2) (def ^:private TY-NULL 5)

;; --- connection --------------------------------------------------------------
(defn open
  "Open `path` (a file or \":memory:\"). Returns the sqlite3* pointer."
  [path]
  (let [pp (ffi/alloc (ffi/sizeof :pointer))]
    (try
      (let [rc (sqlite3-open path pp)
            db (ffi/read pp :pointer)]
        (when-not (= rc SQLITE-OK)
          (throw (ex-info (str "sqlite open failed: " path) {:rc rc})))
        db)
      (finally (ffi/free pp)))))

(defn close [db] (sqlite3-close db) nil)

(defn- bind-params! [stmt params]
  (loop [i 1 ps (seq params)]
    (when ps
      (let [v (first ps)]
        (cond
          (nil? v)                       (sqlite3-bind-null stmt i)
          (and (integer? v) (int? v))    (sqlite3-bind-int64 stmt i v)
          (number? v)                    (sqlite3-bind-double stmt i (double v))
          (string? v)                    (sqlite3-bind-text stmt i v -1 SQLITE-TRANSIENT)
          :else                          (sqlite3-bind-text stmt i (str v) -1 SQLITE-TRANSIENT)))
      (recur (inc i) (next ps)))))

(defn- read-row [stmt n]
  (loop [i 0 m {}]
    (if (= i n)
      m
      (let [k (keyword (sqlite3-column-name stmt i))
            ty (sqlite3-column-type stmt i)
            v (cond
                (= ty TY-INT)   (sqlite3-column-int64 stmt i)
                (= ty TY-FLOAT) (sqlite3-column-double stmt i)
                (= ty TY-NULL)  nil
                :else           (sqlite3-column-text stmt i))]
        (recur (inc i) (assoc m k v))))))

(defn query
  "Run `sql` with `params` (a seq); return a vector of keyword-keyed row maps
  (empty for a non-SELECT)."
  [db sql params]
  (let [pp (ffi/alloc (ffi/sizeof :pointer))
        rc (sqlite3-prepare db sql -1 pp ffi/null)
        stmt (ffi/read pp :pointer)]
    (ffi/free pp)
    (when-not (= rc SQLITE-OK)
      (throw (ex-info (str "sqlite prepare failed: " (sqlite3-errmsg db) " — " sql) {})))
    (bind-params! stmt params)
    (let [ncol (sqlite3-column-count stmt)]
      (loop [rows (transient [])]
        (let [r (sqlite3-step stmt)]
          (cond
            (= r SQLITE-ROW)  (recur (conj! rows (read-row stmt ncol)))
            (= r SQLITE-DONE) (do (sqlite3-finalize stmt) (persistent! rows))
            :else (let [msg (sqlite3-errmsg db)]
                    (sqlite3-finalize stmt)
                    (throw (ex-info (str "sqlite step failed: " msg) {:rc r})))))))))

(defn changes [db] (sqlite3-changes db))
(defn last-insert-rowid [db] (sqlite3-last-rowid db))
