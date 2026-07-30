#!/usr/bin/env bash
#
# End-to-end smoke test for the Iceberg REST catalog endpoints served by
# nexora-app (Phase 4). Exercises the catalog v1 API over HTTP and validates
# the JSON responses.
#
# Prerequisites:
#   - nexora-app built with:  --features event-first,event-streaming,library
#   - A running instance, e.g.:
#       nexora --config config/examples/nexora.iceberg-rest.toml \
#              --allow-unauthenticated \
#              --event-store-backend rest \
#              --event-store-rest-uri http://localhost:8080/api/iceberg/catalog \
#              --event-store-rest-warehouse nexora \
#              --event-store-s3-endpoint http://localhost:9000 \
#              --event-store-s3-bucket nexora-events \
#              --event-store-s3-access-key minioadmin \
#              --event-store-s3-secret-key minioadmin \
#              --event-store-s3-path-style
#
# Usage:
#   ./scripts/test-iceberg-catalog.sh [BASE_URL]
#
# BASE_URL defaults to http://localhost:8080

set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
CATALOG="${BASE_URL}/api/iceberg/catalog/v1"
PASS=0
FAIL=0

# ANSI colors (fall back to plain if not a tty)
if [ -t 1 ]; then
    GREEN='\033[0;32m'; RED='\033[0;31m'; BLUE='\033[0;34m'; NC='\033[0m'
else
    GREEN=''; RED=''; BLUE=''; NC=''
fi

log()  { printf "${BLUE}==>${NC} %s\n" "$1"; }
ok()   { printf "${GREEN} ✓${NC} %s\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "${RED} ✗${NC} %s\n" "$1"; FAIL=$((FAIL+1)); }

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "error: required tool '$1' not found in PATH" >&2
        exit 2
    }
}

require curl

# Prefer jq for JSON assertions; degrade gracefully if absent.
HAVE_JQ=0
if command -v jq >/dev/null 2>&1; then HAVE_JQ=1; fi

# GET a URL, echo body, and assert HTTP status. Args: url expected_status
http_get() {
    local url="$1" expected="$2"
    local resp code body
    resp="$(curl -sS -w '\n%{http_code}' "$url")"
    code="$(printf '%s' "$resp" | tail -n1)"
    body="$(printf '%s' "$resp" | sed '$d')"
    printf '%s' "$body"
    [ "$code" = "$expected" ]
}

log "Testing Iceberg REST catalog at ${CATALOG}"
echo

# ---------------------------------------------------------------------------
# Test 1: GET /v1/config
# ---------------------------------------------------------------------------
log "Test 1: GET /v1/config"
if body="$(http_get "${CATALOG}/config" 200)"; then
    if [ "$HAVE_JQ" = "1" ]; then
        if printf '%s' "$body" | jq -e 'type == "object"' >/dev/null 2>&1; then
            ok "config returns a JSON object"
        else
            bad "config response is not a JSON object: $body"
        fi
    else
        ok "config returned HTTP 200 (install jq for JSON assertions)"
    fi
else
    bad "config did not return HTTP 200"
fi
echo

# ---------------------------------------------------------------------------
# Test 2: GET /v1/namespaces
# ---------------------------------------------------------------------------
log "Test 2: GET /v1/namespaces"
if body="$(http_get "${CATALOG}/namespaces" 200)"; then
    if [ "$HAVE_JQ" = "1" ]; then
        if printf '%s' "$body" | jq -e 'has("namespaces")' >/dev/null 2>&1; then
            n="$(printf '%s' "$body" | jq '.namespaces | length')"
            ok "namespaces response has 'namespaces' array (count=${n})"
        else
            bad "namespaces response missing 'namespaces' key: $body"
        fi
    else
        ok "namespaces returned HTTP 200"
    fi
else
    bad "namespaces did not return HTTP 200"
fi
echo

# ---------------------------------------------------------------------------
# Test 3: GET /v1/namespaces/public/tables
# ---------------------------------------------------------------------------
log "Test 3: GET /v1/namespaces/public/tables"
if body="$(http_get "${CATALOG}/namespaces/public/tables" 200)"; then
    if [ "$HAVE_JQ" = "1" ]; then
        if printf '%s' "$body" | jq -e 'has("identifiers")' >/dev/null 2>&1; then
            n="$(printf '%s' "$body" | jq '.identifiers | length')"
            ok "tables response has 'identifiers' array (count=${n})"
        else
            bad "tables response missing 'identifiers' key: $body"
        fi
    else
        ok "tables returned HTTP 200"
    fi
else
    bad "tables did not return HTTP 200"
fi
echo

# ---------------------------------------------------------------------------
# Test 4: GET a non-existent table -> 404
# ---------------------------------------------------------------------------
log "Test 4: GET /v1/namespaces/public/tables/__does_not_exist__ (expect 404)"
if http_get "${CATALOG}/namespaces/public/tables/__does_not_exist__" 404 >/dev/null; then
    ok "missing table returns HTTP 404"
else
    bad "missing table did not return HTTP 404"
fi
echo

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "----------------------------------------"
printf "Results: ${GREEN}%d passed${NC}, ${RED}%d failed${NC}\n" "$PASS" "$FAIL"
echo "----------------------------------------"

[ "$FAIL" -eq 0 ]
