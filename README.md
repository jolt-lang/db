# jdbc.core for jolt

A SQLite and PostgreSQL database library for [jolt](https://github.com/jolt-lang/jolt)
(Clojure on Chez Scheme). It binds the system **libsqlite3** and **libpq**
directly through `jolt.ffi` — jolt's foreign-function interface — and runs the
real [clojure.jdbc](https://github.com/yogthos/clojure.jdbc) on top of them, plus
a small [next.jdbc](https://github.com/seancorfield/next-jdbc) surface. No jolt
built-in, no JVM: the native binding lives here, and the API is the published
library rather than a copy of it.

`jdbc.core` is clojure.jdbc itself. This library supplies the `java.sql` surface
it drives (`db.jdbc-shim`) over the native drivers, so its own documentation and
semantics apply as written.

```clojure
(require '[db.jdbc])                                       ; registers the shim, once
(require '[jdbc.core :as jdbc])
(with-open [conn (jdbc/connection "sqlite::memory:")]      ; or "postgres://user:pw@host/db"
  (jdbc/execute! conn "create table p (id integer primary key, name text)")
  (jdbc/insert! conn :p {:name "ada"})                     ; -> (1), one result per row
  (jdbc/fetch conn ["select * from p where name = ?" "ada"]))
```

Require `db.jdbc` once before `jdbc.core`, and before anything else that pulls it
in. It has to be loaded first because clojure.jdbc's namespaces resolve the
`java.sql` constants as they compile, and it is what points connection
construction at the native drivers instead of `DriverManager`.

`fetch`/`fetch-one`, `execute!`, `insert!`/`insert-multi!`/`update!`/`delete!`,
`prepared-statement`, and `atomic` (transactions with nested savepoints) are
supported on both backends. Queries are strings or sqlvecs (`[sql & params]`,
JDBC `?` placeholders — rewritten to `$N` for postgres).

Generated keys come back through `RETURNING`, since neither driver has a JDBC
generated-keys channel. `{:returning true}` (or `:all`) asks for the whole row,
which is what postgres' own driver gives; a sequence of column names asks for
those. Without it there are no generated keys to report, so `insert!` falls back
to the update count, exactly as clojure.jdbc does on a driver that has none.

## Binary values

A byte array parameter binds as a SQLite `blob` / postgres `bytea`, and those
columns read back as byte arrays. The bytes round-trip exactly, so embedded NULs,
non-UTF-8 bytes, and empty payloads all survive.

```clojure
(jdbc/execute! conn "create table doc (id integer primary key, body blob)")
(jdbc/insert! conn :doc {:body (byte-array [0 255 65])})
(:body (jdbc/fetch-one conn "select body from doc"))   ; -> byte array
```

On postgres a byte array is sent in binary with its type given as `bytea`, so it
does not depend on the statement offering a `bytea` column for the server to infer
one from. `["select ? as c" (byte-array [1 2])]` binds a `bytea` and reads back as
bytes rather than inferring text.

## Errors

Database errors satisfy `(catch java.sql.SQLException ...)`, so code written
against the JDBC contract works unchanged. Migratus depends on this: its
`table-exists?` probe catches `SQLException` to decide whether it still needs to
create `schema_migrations`. Errors raised by the drivers themselves also carry
`:jdbc/sql-error true` in their `ex-data`.

## Layout

- `db.sqlite` / `db.pg` — the native drivers (jolt.ffi bindings).
- `db.jdbc-shim` — the `java.sql` surface clojure.jdbc drives, over those drivers.
- `db.jdbc` — the entry point: loads the shim, then clojure.jdbc on top of it.
- `jdbc.core` — clojure.jdbc itself, pulled in as a dependency.
- `next.jdbc` (+ `.sql`/`.prepare`/`.result-set`/`.transaction`) — the next.jdbc
  surface migratus and similar tools use.

## Requirements

`jolt` **v0.7.3 or newer** on PATH; the system `libsqlite3` (preinstalled on macOS
and most Linux distros). PostgreSQL support additionally needs `libpq` at runtime.

The version floor is not cosmetic. The shim needs three host fixes that landed in
v0.7.3: `with-open` on a `reify`, a parenthesised `(Class/FIELD)` reading the
field, and a protocol extended to a library-declared class actually dispatching.
On an older jolt this library fails at load or at the first connection.

## Test

```bash
jolt -M:test                                   # sqlite
JOLT_TEST_PG_URI=postgres://... jolt -M:test   # also runs the postgres suite
```
