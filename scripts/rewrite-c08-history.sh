#!/usr/bin/env bash
set -Eeuo pipefail

: "${C08_SECRET_FILE:?Set C08_SECRET_FILE to a mode-0600 file containing only EXPOSED_DATABASE_CREDENTIAL}"
: "${C08_RECOVERY_BUNDLE:?Set C08_RECOVERY_BUNDLE to an administrator-protected backup bundle path}"

command -v git-filter-repo >/dev/null 2>&1 || {
  echo 'git-filter-repo is required.' >&2
  exit 1
}

test "$(git rev-parse --is-bare-repository)" = true || {
  echo 'Run this script only inside a fresh --mirror clone, never a working repository.' >&2
  exit 1
}
test -s "$C08_SECRET_FILE" || { echo 'C08_SECRET_FILE is missing or empty.' >&2; exit 1; }
test "$(stat -c '%a' "$C08_SECRET_FILE")" = 600 || {
  echo 'C08_SECRET_FILE must have mode 0600.' >&2
  exit 1
}
test ! -e "$C08_RECOVERY_BUNDLE" || { echo 'Refusing to overwrite the recovery bundle.' >&2; exit 1; }

origin_url="$(git remote get-url origin)"
umask 077
git bundle create "$C08_RECOVERY_BUNDLE" --all
git for-each-ref --format='%(refname) %(objectname)' refs/heads refs/tags > "${C08_RECOVERY_BUNDLE}.refs"

replacement_file="$(mktemp)"
trap 'rm -f "$replacement_file"' EXIT
printf 'literal:%s==>EXPOSED_DATABASE_CREDENTIAL_REMOVED\n' "$(cat "$C08_SECRET_FILE")" > "$replacement_file"

git filter-repo --force --sensitive-data-removal \
  --replace-text "$replacement_file" \
  --invert-paths \
  --path Test.java \
  --path TestDb.java \
  --path tantor-server/InjectCluster.java \
  --path tantor-server/src/test/java/DeleteConnections.java \
  --path tantor-server/last_call.json \
  --path tantor-server/apply.py \
  --path tantor-server/extract.py \
  --path tantor-server/patch_json.py \
  --path-glob 'tantor-ui/analyze_lint*.py' \
  --path-glob 'tantor-ui/patch_*.py' \
  --path tantor-ui/view_cd.py \
  --path-glob 'tantor-ui/lint_*.json'

if git rev-list --all | while read -r commit; do
  git grep -I -q -F -f "$C08_SECRET_FILE" "$commit" -- && exit 0
done; then
  echo 'The exposed credential remains in rewritten history.' >&2
  exit 1
fi
if git rev-list --objects --all | grep -Eq ' (Test(?:Db)?\.java|tantor-server/(InjectCluster\.java|last_call\.json|apply\.py|extract\.py|patch_json\.py)|tantor-server/src/test/java/DeleteConnections\.java|tantor-ui/(analyze_lint[^/]*\.py|patch_[^/]*\.py|view_cd\.py|lint_[^/]*\.json))$'; then
  echo 'A prohibited developer artifact remains in rewritten history.' >&2
  exit 1
fi

git remote remove origin 2>/dev/null || true
git remote add origin "$origin_url"
echo 'Local mirror rewrite verified. Run Gitleaks against all history before an administrator force-pushes the mirror.'
echo 'The recovery bundle contains the compromised history; keep it access-restricted and destroy it after the rollback window.'
