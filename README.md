# jdbc.core for jolt

A SQLite and PostgreSQL database library for [jolt](https://github.com/jolt-lang/jolt)
(Clojure on Chez Scheme). It binds the system **libsqlite3** and **libpq**
directly through `jolt.ffi` — jolt's foreign-function interface — and exposes the
[clojure.jdbc](https://github.com/yogthos/clojure.jdbc) API plus a small
[next.jdbc](https://github.com/seancorfield/next-jdbc) surface. No jolt built-in,
no JVM: the native binding lives in this library.

```clojure
(require '[jdbc.core :as jdbc])
(with-open [conn (jdbc/connection "sqlite::memory:")]      ; or "postgres://user:pw@host/db"
  (jdbc/execute! conn "create table p (id integer primary key, name text)")
  (jdbc/insert! conn :p {:name "ada"})                     ; -> generated id
  (jdbc/fetch conn ["select * from p where name = ?" "ada"]))
```

`fetch`/`fetch-one`, `execute!`, `insert!`/`insert-multi!`/`update!`/`delete!`,
`last-insert-id`, and `atomic` (transactions with nested savepoints) are
supported on both backends. Queries are strings or sqlvecs (`[sql & params]`,
JDBC `?` placeholders — rewritten to `$N` for postgres).

## Layout

- `db.sqlite` / `db.pg` — the native drivers (jolt.ffi bindings).
- `jdbc.core` — the clojure.jdbc API over them.
- `next.jdbc` (+ `.sql`/`.prepare`/`.result-set`/`.transaction`) — the next.jdbc
  surface migratus and similar tools use.

## Requirements

`joltc` on PATH; the system `libsqlite3` (preinstalled on macOS and most Linux
distros). PostgreSQL support additionally needs `libpq` at runtime.

## Test

```bash
joltc -M:test                              # sqlite
JOLT_TEST_PG_URI=postgres://... joltc -M:test   # also runs the postgres suite
```
