#!/bin/sh
# Run clojure.jdbc's own test suite against db.jdbc-shim.
#
# The tests come from upstream's tree at the sha deps.edn already pins, so this
# gate and the library under test can never disagree about which clojure.jdbc is
# meant. Vendoring the test files here would drift: they would keep passing
# against whatever the shim had become.
#
# Upstream ships its tests outside :paths, so they cannot be reached by adding
# an alias to db's own deps.edn. A throwaway project is assembled instead, with
# db as a :local/root and the checkout's test/ directory on the source roots.
set -eu

db_root=$(cd "$(dirname "$0")/.." && pwd)
work=${TMPDIR:-/tmp}/jdbc-conformance.$$
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/checkout" "$work/project"

# The coordinates come out of deps.edn rather than being repeated here: a bump
# to the clojure.jdbc pin must move this gate with it, not leave it testing an
# older tree that still passes.
read_dep() {
  jolt -e "(let [d (:deps (read-string (slurp \"$db_root/deps.edn\")))
                 e (val (first (filter (fn [[k _]] (= (name k) \"clojure.jdbc\")) d)))]
             (println ($1 e)))"
}
url=$(read_dep :git/url)
sha=$(read_dep :git/sha)
[ -n "$url" ] && [ -n "$sha" ] || { echo "could not read the clojure.jdbc pin from deps.edn" >&2; exit 1; }
echo "clojure.jdbc conformance: $url @ $sha"

# Fetch the one commit rather than cloning the history.
cd "$work/checkout"
git init -q .
git remote add origin "$url"
git fetch -q --depth 1 origin "$sha"
git checkout -q FETCH_HEAD
[ -d test ] || { echo "no test/ directory at $sha" >&2; exit 1; }

cat > "$work/project/deps.edn" <<DEPS
{:paths ["$db_root/conformance"]
 :deps {jolt-lang/db {:local/root "$db_root"}}
 :aliases {:test {:extra-paths ["$work/checkout/test"]
                  :main-opts ["-m" "jdbc-conformance"]}}}
DEPS

cd "$work/project"
exec jolt -M:test
