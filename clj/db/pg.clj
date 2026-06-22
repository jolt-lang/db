(ns db.pg
  "PostgreSQL driver for jolt, binding the system libpq through jolt.ffi. Exposes
  the surface jdbc.core needs: connect / close / exec / all (rows as keyword-keyed
  maps, numeric columns coerced to jolt numbers). Loaded lazily by jdbc.core, so a
  sqlite-only app never needs libpq present."
  (:require [jolt.ffi :as ffi]))

;; libpq is declared in deps.edn (:jolt/native, :optional) and loaded by jolt at
;; startup when present; jdbc.core only requires this namespace for a postgres
;; connection, so a sqlite-only app never needs libpq.

(ffi/defcfn PQconnectdb        "PQconnectdb"        [:string] :pointer)
(ffi/defcfn PQstatus           "PQstatus"           [:pointer] :int)
(ffi/defcfn PQerrorMessage     "PQerrorMessage"     [:pointer] :string)
(ffi/defcfn PQfinish           "PQfinish"           [:pointer] :void)
(ffi/defcfn PQexecParams       "PQexecParams"       [:pointer :string :int :pointer :pointer :pointer :pointer :int] :pointer)
(ffi/defcfn PQresultStatus     "PQresultStatus"     [:pointer] :int)
(ffi/defcfn PQresultErrorMessage "PQresultErrorMessage" [:pointer] :string)
(ffi/defcfn PQntuples          "PQntuples"          [:pointer] :int)
(ffi/defcfn PQnfields          "PQnfields"          [:pointer] :int)
(ffi/defcfn PQfname            "PQfname"            [:pointer :int] :string)
(ffi/defcfn PQftype            "PQftype"            [:pointer :int] :uint)
(ffi/defcfn PQgetvalue         "PQgetvalue"         [:pointer :int :int] :string)
(ffi/defcfn PQgetisnull        "PQgetisnull"        [:pointer :int :int] :int)
(ffi/defcfn PQclear            "PQclear"            [:pointer] :void)

(def ^:private CONNECTION-OK 0)
(def ^:private PGRES-COMMAND-OK 1)
(def ^:private PGRES-TUPLES-OK 2)
;; column type Oids worth coercing back to jolt numbers/booleans
(def ^:private int-oids #{20 21 23})            ; int8 / int2 / int4 (+ serial)
(def ^:private float-oids #{700 701 1700})      ; float4 / float8 / numeric
(def ^:private bool-oid 16)

(defn connect [uri]
  (let [conn (PQconnectdb uri)]
    (when-not (= CONNECTION-OK (PQstatus conn))
      (let [msg (PQerrorMessage conn)] (PQfinish conn)
        (throw (ex-info (str "pg connect failed: " msg) {}))))
    conn))

(defn close [conn] (PQfinish conn) nil)

;; build a char** of the params' text representations (NULL for a nil param);
;; returns [arr-ptr str-ptrs] to free after the call.
(defn- param-array [params]
  (let [n (count params)
        ps (ffi/sizeof :pointer)
        arr (if (zero? n) ffi/null (ffi/alloc (* n ps)))
        strs (mapv (fn [v] (if (nil? v) ffi/null (ffi/string->ptr (str v)))) params)]
    (dotimes [i n] (ffi/write arr :pointer (* i ps) (nth strs i)))
    [arr strs]))

(defn- run [conn sql params]
  (let [[arr strs] (param-array params)
        res (PQexecParams conn sql (count params) ffi/null arr ffi/null ffi/null 0)]
    (doseq [p strs] (when-not (ffi/null? p) (ffi/free p)))
    (when-not (ffi/null? arr) (ffi/free arr))
    (let [st (PQresultStatus res)]
      (when-not (or (= st PGRES-COMMAND-OK) (= st PGRES-TUPLES-OK))
        (let [msg (PQresultErrorMessage res)] (PQclear res)
          (throw (ex-info (str "pg query failed: " msg) {:sql sql}))))
      res)))

(defn exec [conn sql params] (PQclear (run conn sql params)) nil)

(defn- coerce [oid s]
  (cond (int-oids oid)   (parse-long s)
        (float-oids oid) (parse-double s)
        (= bool-oid oid) (= s "t")
        :else            s))

(defn all [conn sql params]
  (let [res (run conn sql params)
        nrows (PQntuples res)
        ncols (PQnfields res)
        cols (mapv (fn [c] [(keyword (PQfname res c)) (PQftype res c)]) (range ncols))
        rows (mapv (fn [r]
                     (reduce (fn [m c]
                               (let [[k oid] (nth cols c)]
                                 (assoc m k (if (zero? (PQgetisnull res r c)) (coerce oid (PQgetvalue res r c)) nil))))
                             {} (range ncols)))
                   (range nrows))]
    (PQclear res)
    rows))
