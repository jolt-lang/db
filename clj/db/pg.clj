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

;; --- bytea ↔ byte-array ------------------------------------------------------
;; PQexecParams runs in text mode here (NULL paramFormats, result format 0), so
;; bytea crosses the wire as text in both directions. Outbound a byte-array
;; becomes a hex literal, which postgres has parsed for bytea input since 9.0.
;; Inbound the encoding is the server's bytea_output: hex since 9.0, escape on
;; older servers or where it's been set back, so decode handles both. Either way
;; the text is pure ASCII (escape output octal-escapes everything outside
;; printable ASCII), so latin1 recovers the bytes libpq gave us byte for byte.

(def ^:private hex-digits ["0" "1" "2" "3" "4" "5" "6" "7" "8" "9" "a" "b" "c" "d" "e" "f"])

(defn- bytes->hex-literal [arr]
  (str "\\x" (apply str (map (fn [b]
                               (let [v (bit-and b 0xff)]
                                 (str (nth hex-digits (bit-shift-right v 4))
                                      (nth hex-digits (bit-and v 0xf)))))
                             arr))))

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

;; build a char** of the params' text representations (NULL for a nil param,
;; a hex literal for a byte-array); returns [arr-ptr str-ptrs] to free after
;; the call.
(defn- param-array [params]
  (let [n (count params)
        ps (ffi/sizeof :pointer)
        arr (if (zero? n) ffi/null (ffi/alloc (* n ps)))
        strs (mapv (fn [v] (cond
                             (nil? v)   ffi/null
                             (bytes? v) (ffi/string->ptr (bytes->hex-literal v))
                             :else      (ffi/string->ptr (str v))))
                   params)]
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
