(ns db.pg
  "PostgreSQL driver for jolt, binding the system libpq through jolt.ffi. Exposes
  the surface jdbc.core needs: connect / close / exec / all (rows as keyword-keyed
  maps, numeric columns coerced to jolt numbers). Loaded lazily by jdbc.core, so a
  sqlite-only app never needs libpq present."
  (:require [jolt.ffi :as ffi]
            [clojure.string :as str]))

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
(ffi/defcfn PQcmdTuples         "PQcmdTuples"         [:pointer] :string)
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

;;; ? -> $N rewriting
;;
;; Which ? counts as a placeholder decides which parameter lands where, so a ?
;; that only looks like one has to be skipped without consuming a number. That
;; means recognising the constructs a ? can hide in: string literals, quoted
;; identifiers, line and block comments, dollar-quoted bodies, and E'' escape
;; strings. Each is skipped whole by the scanner below.

(defn- at
  "The character at `i`, or nil past the end, so callers can compare without
  bounds-checking first."
  [sql len i]
  (when (< i len) (nth sql i)))

(defn- digit? [c] (let [x (int c)] (and (>= x 48) (<= x 57))))

(defn- ident-char? [c]
  (let [x (int c)]
    (or (and (>= x 97) (<= x 122))              ; a-z
        (and (>= x 65) (<= x 90))               ; A-Z
        (and (>= x 48) (<= x 57))               ; 0-9
        (= x 95)                                ; _
        (> x 127))))                            ; postgres allows non-ascii here

(defn- token-start?
  "True when `i` begins a token rather than continuing an identifier. Postgres
  allows $ inside an identifier after the first character, so a$b$c is one name
  and not a dollar quote opening, and E only introduces an escape string when it
  stands alone rather than ending a word like date'2020-01-01'. Its own lexer
  draws the line in the same place."
  [sql i]
  (or (zero? i)
      (let [p (nth sql (dec i))]
        (not (or (ident-char? p) (= p \$))))))

(defn- skip-quoted
  "Index just past the run of quote character `q` opening at `i`. A doubled quote
  is an escaped one; `escapes?` additionally honours backslash escapes, which is
  what distinguishes E'...' from a plain literal. An unterminated run ends at the
  end of the statement rather than throwing."
  [sql len i q escapes?]
  (loop [j (inc i)]
    (cond
      (>= j len)                        len
      (and escapes? (= (nth sql j) \\)) (recur (+ j 2))
      (not= (nth sql j) q)              (recur (inc j))
      (= (at sql len (inc j)) q)        (recur (+ j 2))
      :else                             (inc j))))

(defn- skip-line-comment [sql len i]
  (loop [j (+ i 2)]
    (cond (>= j len)                 len
          (= (nth sql j) \newline)   j
          :else                      (recur (inc j)))))

(defn- skip-block-comment
  "Index just past the /* */ comment opening at `i`. Postgres nests these, so
  track depth rather than stopping at the first */."
  [sql len i]
  (loop [j (+ i 2) depth 1]
    (cond
      (>= j len) len
      (and (= (nth sql j) \/) (= (at sql len (inc j)) \*)) (recur (+ j 2) (inc depth))
      (and (= (nth sql j) \*) (= (at sql len (inc j)) \/)) (if (= depth 1)
                                                             (+ j 2)
                                                             (recur (+ j 2) (dec depth)))
      :else (recur (inc j) depth))))

(defn- dollar-tag-len
  "Length of the $tag$ that opens at `i`, or nil when this $ does not open a
  dollar quote. The tag follows unquoted-identifier rules, so it cannot start
  with a digit, which is what keeps a positional $1 from being read as one."
  [sql len i]
  (loop [j (inc i)]
    (let [c (at sql len j)]
      (cond
        (nil? c)                        nil
        (= c \$)                        (- (inc j) i)
        (and (= j (inc i)) (digit? c))  nil
        (ident-char? c)                 (recur (inc j))
        :else                           nil))))

(defn- skip-dollar-quoted [sql len i taglen]
  (let [tag (subs sql i (+ i taglen))]
    (if-let [close (str/index-of sql tag (+ i taglen))]
      (+ close taglen)
      len)))

(defn pg-placeholders
  "JDBC ? placeholders -> postgres $1..$N. A ? inside a string literal, quoted
  identifier, comment, dollar-quoted body or escape string is left as it is and
  does not consume a number. Collects the pieces and joins them once, so the cost
  is linear in the length of the statement."
  [sql]
  (let [len (count sql)]
    (loop [i 0 from 0 pnum 1 pieces (transient [])]
      (if (>= i len)
        (str/join (persistent! (conj! pieces (subs sql from len))))
        (let [c (nth sql i)
              nxt (at sql len (inc i))]
          (cond
            (= c \?)
            (recur (inc i) (inc i) (inc pnum)
                   (conj! (conj! pieces (subs sql from i)) (str "$" pnum)))

            (= c \') (recur (skip-quoted sql len i \' false) from pnum pieces)
            (= c \") (recur (skip-quoted sql len i \" false) from pnum pieces)

            ;; E'...' / e'...', where a backslash escapes the next character
            (and (or (= c \E) (= c \e)) (= nxt \') (token-start? sql i))
            (recur (skip-quoted sql len (inc i) \' true) from pnum pieces)

            (and (= c \-) (= nxt \-)) (recur (skip-line-comment sql len i) from pnum pieces)
            (and (= c \/) (= nxt \*)) (recur (skip-block-comment sql len i) from pnum pieces)

            (= c \$)
            (if-let [taglen (and (token-start? sql i) (dollar-tag-len sql len i))]
              (recur (skip-dollar-quoted sql len i taglen) from pnum pieces)
              (recur (inc i) from pnum pieces))

            :else (recur (inc i) from pnum pieces)))))))

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
        ;; callers write JDBC ? placeholders; postgres wants $1..$N
        res (PQexecParams conn (pg-placeholders sql) (count params)
                          types values lengths formats 0)]
    (doseq [p owned] (ffi/free p))
    (let [st (PQresultStatus res)]
      (when-not (or (= st PGRES-COMMAND-OK) (= st PGRES-TUPLES-OK))
        (let [msg (PQresultErrorMessage res)] (PQclear res)
          (throw (ex-info (str "pg query failed: " msg)
                          {:sql sql :jdbc/sql-error true}))))
      res)))

(defn exec
  "Run a statement and return the number of rows it affected. Commands that do not
  report a count, DDL among them, give 0 — which is what sqlite3_changes reports
  for those too."
  [conn sql params]
  (let [res (run conn sql params)
        n (PQcmdTuples res)]                     ; must be read before PQclear
    (PQclear res)
    (or (when n (parse-long n)) 0)))

(defn- coerce [oid s]
  (cond (int-oids oid)    (parse-long s)
        (float-oids oid)  (parse-double s)
        (= bool-oid oid)  (= s "t")
        (= bytea-oid oid) (bytea->bytes s)
        :else             s))

(defn all-raw
  "Run `sql` and return {:labels [col-name ...] :rows [[v ...]]}, keeping column
  order for a caller that reads a row by index. `all` is this with the rows turned
  into maps."
  [conn sql params]
  (let [res (run conn sql params)
        nrows (PQntuples res)
        ncols (PQnfields res)
        labels (mapv (fn [c] (PQfname res c)) (range ncols))
        oids (mapv (fn [c] (PQftype res c)) (range ncols))
        rows (mapv (fn [r]
                     (mapv (fn [c]
                             (when (zero? (PQgetisnull res r c))
                               (coerce (nth oids c) (PQgetvalue res r c))))
                           (range ncols)))
                   (range nrows))]
    (PQclear res)
    {:labels labels :rows rows}))

(defn all [conn sql params]
  (let [{:keys [labels rows]} (all-raw conn sql params)
        ks (mapv keyword labels)]
    (mapv (fn [vs] (zipmap ks vs)) rows)))
