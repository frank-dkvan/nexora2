#!/usr/bin/env bash
# Local stand-in for .github/workflows/ci.yml, for when GitHub Actions is
# unavailable (e.g. Actions quota exhausted on a private repo).
#
# Runs the same checks the CI workflow runs, as native commands. Mirrors the
# jobs in ci.yml one-to-one so "green locally" means the same thing the cloud
# check would have meant — with the platform caveats noted per section.
#
# PLATFORM CAVEAT (read this): CI runs on ubuntu-latest; this script runs on
# whatever you're on (likely macOS). Logic checks (test/clippy/fmt/features/
# audit) are trustworthy cross-platform. The Docker and cluster-e2e sections
# are platform-sensitive — a macOS pass does NOT guarantee the Linux CI would
# pass. For a release you intend to run in production (Linux), re-run at least
# the Docker section on Linux or in the container itself.
#
# Usage:
#   scripts/local-ci.sh            # run the trustworthy core (test/lint/features/audit)
#   scripts/local-ci.sh --all      # also run Docker build + cluster e2e (slow, platform-sensitive)
#   scripts/local-ci.sh --docker   # core + Docker only
#   scripts/local-ci.sh --e2e      # core + cluster e2e only
#
# Exit code is non-zero if any run section fails. Sections are independent:
# the script runs them all and reports a summary at the end rather than
# stopping at the first failure, so one run surfaces every problem.

set -uo pipefail
cd "$(dirname "$0")/.."

RUN_DOCKER=0
RUN_E2E=0
case "${1:-}" in
  --all)    RUN_DOCKER=1; RUN_E2E=1 ;;
  --docker) RUN_DOCKER=1 ;;
  --e2e)    RUN_E2E=1 ;;
  "")       ;;
  *) echo "unknown arg: $1 (use --all | --docker | --e2e | none)"; exit 2 ;;
esac

# --- result tracking -------------------------------------------------------
declare -a NAMES=()
declare -a CODES=()
run() {
  # run "<label>" <cmd...> — echo a banner, run, record the exit code.
  local label="$1"; shift
  echo ""
  echo "=============================================================="
  echo ">> $label"
  echo "   \$ $*"
  echo "=============================================================="
  "$@"
  local code=$?
  NAMES+=("$label")
  CODES+=("$code")
  if [ $code -ne 0 ]; then
    echo ">> FAILED ($label) exit=$code"
  fi
  return 0  # never abort; we want the full summary
}

# ==========================================================================
# JOB: Test & Lint   (ci.yml job "test") — trustworthy on any platform
# ==========================================================================
run "test/build (--all-targets)"        cargo build --all-targets
run "test/test (--workspace)"           cargo test --workspace
run "test/clippy (-D warnings)"         cargo clippy --all-targets -- -D warnings
run "test/fmt (--check)"                cargo fmt -- --check

# ==========================================================================
# JOB: Feature Combinations   (ci.yml job "feature-matrix") — trustworthy
#   Matrix in CI: "" (no features / lite) and "default".
# ==========================================================================
run "feature/lite build (--no-default-features)"   cargo build --workspace --no-default-features
run "feature/lite test  (--no-default-features)"   cargo test  --workspace --no-default-features
run "feature/default build"                        cargo build --workspace --no-default-features --features default
run "feature/default test"                         cargo test  --workspace --no-default-features --features default

# ==========================================================================
# JOB: Security Audit   (ci.yml job "security") — trustworthy
#   cargo-audit is pure-Rust; installs on first run if missing.
# ==========================================================================
if ! command -v cargo-audit >/dev/null 2>&1; then
  run "security/install cargo-audit"    cargo install cargo-audit
fi
run "security/audit"                    cargo audit

# ==========================================================================
# JOB: Docker Build   (ci.yml job "docker-build") — PLATFORM-SENSITIVE
#   Image is Linux; needs Docker running. Opt-in via --docker / --all.
# ==========================================================================
if [ "$RUN_DOCKER" -eq 1 ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo ">> SKIP docker: docker not installed"
    NAMES+=("docker (skipped: not installed)"); CODES+=(0)
  else
    run "docker/build"          docker build -t nexora-ci .
    run "docker/verify --help"  docker run --rm nexora-ci --help
  fi
else
  echo ">> skipping Docker Build (pass --docker or --all to run)"
fi

# ==========================================================================
# JOB: Multi-process cluster e2e   (ci.yml job "cluster-e2e") — PLATFORM-SENSITIVE
#   #[ignore] tests that spawn real nexora processes; timing-sensitive.
#   Opt-in via --e2e / --all. Single-threaded, as in CI.
# ==========================================================================
if [ "$RUN_E2E" -eq 1 ]; then
  run "e2e/build acceptance"  cargo test -p nexora-app --test cluster_acceptance --no-run
  run "e2e/build scale-out"   cargo test -p nexora-app --test dynamic_scale_out --no-run
  run "e2e/acceptance matrix" cargo test -p nexora-app --test cluster_acceptance -- --ignored --nocapture --test-threads=1
  run "e2e/scale-out"         cargo test -p nexora-app --test dynamic_scale_out -- --ignored --nocapture --test-threads=1
else
  echo ">> skipping cluster e2e (pass --e2e or --all to run)"
fi

# --- summary ---------------------------------------------------------------
echo ""
echo "=============================================================="
echo "SUMMARY"
echo "=============================================================="
fail=0
for i in "${!NAMES[@]}"; do
  if [ "${CODES[$i]}" -eq 0 ]; then
    printf "  PASS  %s\n" "${NAMES[$i]}"
  else
    printf "  FAIL  %s (exit=%s)\n" "${NAMES[$i]}" "${CODES[$i]}"
    fail=1
  fi
done
echo "=============================================================="
if [ "$fail" -eq 0 ]; then
  echo "ALL PASSED"
else
  echo "SOME CHECKS FAILED — see FAIL lines above"
fi
exit $fail

