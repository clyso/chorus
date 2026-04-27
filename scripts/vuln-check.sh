#!/usr/bin/env bash
# Wrap `go tool govulncheck` and apply .govulncheck-ignore.yaml.
# See that file for the allowlist schema.
#
# Exit codes:
#   0 — clean scan, or every reported vuln has a matching (id+module)
#       allowlist entry.
#   1 — allowlist contains stale entries, or YAML failed schema validation.
#       (Distinct from govulncheck's own non-zero — these are maintenance
#       issues, not vulnerability findings.)
#   non-zero (govulncheck's) — a vuln was reported that the allowlist does
#       not cover.

set -uo pipefail

IGNORE_FILE="${IGNORE_FILE:-.govulncheck-ignore.yaml}"

scan_out=$(mktemp)
found_pairs=$(mktemp)
allowed_pairs=$(mktemp)
trap 'rm -f "$scan_out" "$found_pairs" "$allowed_pairs"' EXIT

go tool govulncheck ./... >"$scan_out" 2>&1
code=$?
cat "$scan_out"

if [ "$code" -eq 0 ]; then
    exit 0
fi

# govulncheck format is fixed: each "Vulnerability #N: GO-..." block is
# followed by a "Found in: <module>@<ver>" line — id is always $3 on the
# header, module path is always $3 (pre-@) on the Found-in line.
awk '
    /^Vulnerability #[0-9]+:/ { id = $3 }
    /^[[:space:]]+Found in:/ {
        m = $3; sub(/@.*/, "", m)
        if (id != "") print id "\t" m
    }
' "$scan_out" | sort -u >"$found_pairs"

# Parse the YAML allowlist into id<TAB>module pairs. The parser:
#   - accepts free key order within an entry (id/module/reason in any sequence)
#   - validates that every entry has a non-empty id, module, and reason
#   - errors clearly on missing/empty fields rather than silently miscounting
if [ -f "$IGNORE_FILE" ]; then
    awk '
        function unquote(v) {
            sub(/^"/, "", v); sub(/"$/, "", v)
            sub(/^'\''/, "", v); sub(/'\''$/, "", v)
            return v
        }
        function flush() {
            if (id == "") {
                printf "error: entry near line %d missing or empty `id`\n", entry_line > "/dev/stderr"
                bad = 1
            } else if (module == "") {
                printf "error: entry %s missing or empty `module`\n", id > "/dev/stderr"
                bad = 1
            } else if (reason == "") {
                printf "error: entry %s missing or empty `reason`\n", id > "/dev/stderr"
                bad = 1
            } else {
                print id "\t" module
            }
            id = ""; module = ""; reason = ""
        }
        /^[[:space:]]*-/ {
            if (have) flush()
            have = 1
            entry_line = NR
        }
        {
            s = $0
            sub(/^[[:space:]]*-?[[:space:]]*/, "", s)
            if (match(s, /^id:[[:space:]]*/)) {
                v = substr(s, RSTART + RLENGTH)
                sub(/[[:space:]]+#.*$/, "", v); sub(/[[:space:]\r]+$/, "", v)
                id = unquote(v)
            } else if (match(s, /^module:[[:space:]]*/)) {
                v = substr(s, RSTART + RLENGTH)
                sub(/[[:space:]]+#.*$/, "", v); sub(/[[:space:]\r]+$/, "", v)
                module = unquote(v)
            } else if (match(s, /^reason:[[:space:]]*/)) {
                v = substr(s, RSTART + RLENGTH)
                sub(/[[:space:]\r]+$/, "", v)
                reason = unquote(v)
            }
        }
        END { if (have) flush(); if (bad) exit 1 }
    ' "$IGNORE_FILE" | sort -u >"$allowed_pairs" || {
        printf '\n>>> Failed to parse %s — fix the schema errors above.\n' "$IGNORE_FILE" >&2
        exit 1
    }
fi

unexpected=$(comm -23 "$found_pairs" "$allowed_pairs")
if [ -n "$unexpected" ]; then
    printf '\n>>> Unexpected vulnerabilities (id\tmodule) not allowlisted in %s:\n%s\n' \
        "$IGNORE_FILE" "$unexpected" >&2
    # If an ID is allowlisted but for a different module, surface the
    # mismatch — the exact failure mode the strict id+module pairing catches.
    awk -F'\t' '
        NR==FNR { allowed_module[$1] = $2; next }
        ($1 in allowed_module) && allowed_module[$1] != $2 {
            printf "    note: %s is allowlisted for module %s but the scanner reports module %s\n",
                $1, allowed_module[$1], $2
        }
    ' "$allowed_pairs" - <<<"$unexpected" >&2
    exit "$code"
fi

stale=$(comm -13 "$found_pairs" "$allowed_pairs")
if [ -n "$stale" ]; then
    # Stale entries mean the scan succeeded but the allowlist has rotted —
    # exit 1 is intentional here, distinct from govulncheck's failure code.
    printf '\n>>> Stale entries in %s (no longer reported by the scanner — please remove):\n%s\n' \
        "$IGNORE_FILE" "$stale" >&2
    exit 1
fi

printf '\n>>> All reported vulnerabilities are allowlisted in %s — PASS\n' "$IGNORE_FILE"
