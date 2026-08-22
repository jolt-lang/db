(ns db.datasource
  "An explicit datasource lifecycle over the native drivers: a datasource is a
  connection FACTORY (a spec plus an open/closed flag), not a pool. acquire
  opens a real connection, release closes it, close-datasource stops further
  acquires. Pooling can grow behind this same surface later; deliberately, this
  library does not emulate HikariCP.

  next.jdbc/get-datasource wraps open-datasource, and every next.jdbc operation
  accepts a datasource where it accepts a connection (opening and closing per
  call, like next.jdbc's connection ownership rules)."
  (:require [jdbc.core :as jc]))

(def ^:private ds-tag :db/datasource)

(clojure.core/__register-class-methods! ds-tag
  {"getConnection" (fn [self] ((jolt.host/ref-get self :acquire)))
   "close"         (fn [self] (jolt.host/ref-put! self :closed true) nil)
   "isClosed"      (fn [self] (boolean (jolt.host/ref-get self :closed)))
   "toString"      (fn [self] (str "db.datasource[" (pr-str (jolt.host/ref-get self :spec)) "]"))})

;; a datasource answers instance? javax.sql.DataSource, which is how library
;; code (next.jdbc itself, HugSQL, ...) distinguishes it from a connection.
(clojure.core/__register-instance-check!
  (fn [cn val]
    (if (and (= cn "javax.sql.DataSource")
             (jolt.host/table? val)
             (= (jolt.host/ref-get val :jolt/type) ds-tag))
      true
      nil)))

(defn datasource? [x]
  (and (jolt.host/table? x) (= (jolt.host/ref-get x :jolt/type) ds-tag)))

(defn open-datasource
  "spec -> datasource. The spec is anything jdbc.core/connection accepts (a uri
  string or a dbspec map); it is validated on first acquire, not here."
  [spec]
  (let [t (jolt.host/tagged-table ds-tag)]
    (jolt.host/ref-put! t :spec spec)
    (jolt.host/ref-put! t :closed false)
    (jolt.host/ref-put! t :acquire
                        (fn [] (when (jolt.host/ref-get t :closed)
                                 (throw (ex-info "datasource is closed" {:spec spec})))
                          (jc/connection spec)))
    t))

(defn acquire
  "Open a connection from the datasource (a raw jdbc.core connection)."
  [ds]
  ((jolt.host/ref-get ds :acquire)))

(defn release
  "Return a connection to the datasource. Without a pool this closes it."
  [_ds conn]
  (.close conn)
  nil)

(defn close-datasource
  "Stop further acquires. Connections already handed out are unaffected."
  [ds]
  (jolt.host/ref-put! ds :closed true)
  nil)

(defn spec-of [ds] (jolt.host/ref-get ds :spec))
