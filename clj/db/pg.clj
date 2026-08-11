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
;; column type Oids worth coercing back to jolt numbers/booleans/byte-arrays
(def ^:private int-oids #{20 21 23})            ; int8 / int2 / int4 (+ serial)
(def ^:private float-oids #{700 701 1700})      ; float4 / float8 / numeric
(def ^:private bool-oid 16)
(def ^:private bytea-oid 17)

(defn connect [uri]
  (let [conn (PQconnectdb uri)]
    (when-not (= CONNECTION-OK (PQstatus conn))
      (let [msg (PQerrorMessage conn)] (PQfinish conn)
        (throw (ex-info (str "pg connect failed: " msg) {:jdbc/sql-error true}))))
    conn))

(defn close [conn] (PQfinish conn) nil)

;; --- bytea → byte-array ------------------------------------------------------
;; Results are still requested in text format (PQexecParams resultFormat 0), so a
;; bytea column arrives as text in whichever encoding bytea_output names: hex
;; since 9.0, escape on older servers or where it has been set back, so decode
;; handles both. Either encoding is pure ASCII (escape output octal-escapes
;; everything outside printable ASCII), so latin1 recovers the bytes libpq gave
;; us byte for byte. Parameters go the other way in binary, see param-arrays.

(defn- hex-nibble [b]
  (cond (and (>= b 48) (<= b 57))  (- b 48)     ; 0-9
        (and (>= b 97) (<= b 102)) (- b 87)     ; a-f
        (and (>= b 65) (<= b 70))  (- b 55)     ; A-F
        :else (throw (ex-info "invalid hex digit in bytea value" {:byte b}))))

(defn- hex->bytes
  "Decode the hex digits of `src` (the latin1 bytes of a bytea value) from `from`."
  [src from]
  (let [n (quot (- (alength src) from) 2)]
    (byte-array (mapv (fn [i]
                        (let [j (+ from (* 2 i))]
                          (+ (* 16 (hex-nibble (aget src j)))
                             (hex-nibble (aget src (inc j))))))
                      (range n)))))

(defn- escape->bytes
  "Decode bytea escape output: \\\\ is a backslash, \\NNN an octal byte, the rest literal."
  [src]
  (let [n (alength src)]
    (loop [i 0 acc (transient [])]
      (if (>= i n)
        (byte-array (persistent! acc))
        (let [b (aget src i)]
          (cond
            (not (and (= b 92) (< (inc i) n))) (recur (inc i) (conj! acc b))
            (= 92 (aget src (inc i)))          (recur (+ i 2) (conj! acc 92))
            :else (recur (+ i 4)
                         (conj! acc (+ (* 64 (- (aget src (inc i)) 48))
                                       (* 8 (- (aget src (+ i 2)) 48))
                                       (- (aget src (+ i 3)) 48))))))))))

(defn- bytea->bytes [s]
  (let [src (.getBytes s "latin1")]
    (if (and (>= (alength src) 2) (= 92 (aget src 0)) (= 120 (aget src 1)))
      (hex->bytes src 2)
      (escape->bytes src))))

;; --- parameters --------------------------------------------------------------
;; PQexecParams takes four parallel per-parameter arrays: types (Oids), values,
;; lengths, and formats. Everything goes over as text with its type left for
;; postgres to infer, which is what the server does with a plain query anyway.
;;
;; A byte array is the exception: it goes over in binary with its Oid stated
;; outright as bytea. Stating the type is the point. It means the parameter
;; carries its own type instead of leaning on the statement to supply a bytea
;; column to infer one from, so `select ?` binds a bytea rather than inferring
;; text and handing back the literal the driver sent. Binary also skips the hex
;; encode on the way out, so a large value crosses once as bytes instead of twice
;; the size in ASCII.
(def ^:private INFER-OID 0)              ; 0 = let postgres infer this parameter
(def ^:private TEXT-FORMAT 0)
(def ^:private BINARY-FORMAT 1)

(defn- param-arrays
  "Build PQexecParams' type / value / length / format arrays for `params`.
  Returns [types values lengths formats owned], where owned is every pointer the
  caller must free once the call has returned."
  [params]
  (let [n (count params)]
    (if (zero? n)
      [ffi/null ffi/null ffi/null ffi/null []]
      (let [ps (ffi/sizeof :pointer)
            is (ffi/sizeof :int)
            os (ffi/sizeof :uint)
            types   (ffi/alloc (* n os))
            values  (ffi/alloc (* n ps))
            lengths (ffi/alloc (* n is))
            formats (ffi/alloc (* n is))
            ;; a NULL value pointer is how libpq reads a SQL NULL, so an empty
            ;; byte array still needs a pointer of its own to stay distinct from
            ;; nil — hence the 1-byte floor on a 0-length payload.
            cells (mapv (fn [v]
                          (cond
                            (nil? v)   [INFER-OID ffi/null 0 TEXT-FORMAT]
                            (bytes? v) (let [len (alength v)
                                             p (ffi/alloc (max 1 len))]
                                         (ffi/write-array p v)
                                         [bytea-oid p len BINARY-FORMAT])
                            :else      [INFER-OID (ffi/string->ptr (str v)) 0 TEXT-FORMAT]))
                        params)]
        (dotimes [i n]
          (let [[oid p len fmt] (nth cells i)]
            (ffi/write types   :uint    (* i os) oid)
            (ffi/write values  :pointer (* i ps) p)
            (ffi/write lengths :int     (* i is) len)
            (ffi/write formats :int     (* i is) fmt)))
        [types values lengths formats
         (into [types values lengths formats]
               (remove (fn [p] (ffi/null? p)) (mapv second cells)))]))))

(defn- run [conn sql params]
  (let [[types values lengths formats owned] (param-arrays params)
        res (PQexecParams conn sql (count params) types values lengths formats 0)]
    (doseq [p owned] (ffi/free p))
    (let [st (PQresultStatus res)]
      (when-not (or (= st PGRES-COMMAND-OK) (= st PGRES-TUPLES-OK))
        (let [msg (PQresultErrorMessage res)] (PQclear res)
          (throw (ex-info (str "pg query failed: " msg)
                          {:sql sql :jdbc/sql-error true}))))
      res)))

(defn exec [conn sql params] (PQclear (run conn sql params)) nil)

(defn- coerce [oid s]
  (cond (int-oids oid)    (parse-long s)
        (float-oids oid)  (parse-double s)
        (= bool-oid oid)  (= s "t")
        (= bytea-oid oid) (bytea->bytes s)
        :else             s))

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
