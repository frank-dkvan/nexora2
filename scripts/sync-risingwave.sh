#!/usr/bin/env bash
# sync-risingwave.sh - Sync RisingWave upstream changes
#
# This script helps synchronize RisingWave updates from upstream into the
# vendor/risingwave Git Subtree.
#
# Usage:
#   ./scripts/sync-risingwave.sh [OPTIONS]
#
# Options:
#   --check              Check for available upstream versions
#   --upgrade VERSION    Upgrade to specific version (e.g., v3.1.0)
#   --dry-run           Show what would be done without making changes
#   -h, --help          Show this help message
#
# Examples:
#   ./scripts/sync-risingwave.sh --check
#   ./scripts/sync-risingwave.sh --upgrade v3.1.0
#   ./scripts/sync-risingwave.sh --upgrade v3.1.0 --dry-run

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
RISINGWAVE_REMOTE="risingwave-upstream"
RISINGWAVE_REPO="https://github.com/risingwavelabs/risingwave.git"
SUBTREE_PREFIX="vendor/risingwave"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Command line options
CHECK_ONLY=false
UPGRADE_VERSION=""
DRY_RUN=false

# Logging
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
error_exit() { log_error "$1"; exit 1; }

# Parse arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --check)
                CHECK_ONLY=true
                shift
                ;;
            --upgrade)
                UPGRADE_VERSION="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            -h|--help)
                sed -n '2,16p' "$0" | sed 's/^# //' | sed 's/^#//'
                exit 0
                ;;
            *)
                error_exit "Unknown option: $1. Use --help for usage."
                ;;
        esac
    done
}

# Check prerequisites
check_prerequisites() {
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        error_exit "Not a git repository"
    fi

    if [ ! -d "$PROJECT_ROOT/$SUBTREE_PREFIX" ]; then
        error_exit "RisingWave subtree not found. Run init-risingwave.sh first."
    fi

    if ! git remote get-url "$RISINGWAVE_REMOTE" > /dev/null 2>&1; then
        log_warning "Remote '$RISINGWAVE_REMOTE' not found. Adding..."
        git remote add "$RISINGWAVE_REMOTE" "$RISINGWAVE_REPO"
    fi
}

# Get current RisingWave version
get_current_version() {
    cd "$PROJECT_ROOT/$SUBTREE_PREFIX"

    # Try to get version from Cargo.toml
    if [ -f "Cargo.toml" ]; then
        local version
        version=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
        echo "$version"
    else
        echo "unknown"
    fi
}

# Fetch available versions
fetch_versions() {
    log_info "Fetching available RisingWave versions..."

    git fetch "$RISINGWAVE_REMOTE" --tags --quiet

    log_success "Fetch complete"
}

# List available versions
list_versions() {
    log_info "Available RisingWave versions (latest 10):"
    echo ""

    git ls-remote --tags "$RISINGWAVE_REMOTE" | \
        grep -o 'refs/tags/v[0-9]*\.[0-9]*\.[0-9]*$' | \
        sed 's|refs/tags/||' | \
        sort -V | \
        tail -10 | \
        while read -r version; do
            echo "  - $version"
        done

    echo ""
    local current
    current=$(get_current_version)
    log_info "Current version: $current"
}

# Check for updates
check_updates() {
    fetch_versions
    list_versions

    echo ""
    log_info "To upgrade, run:"
    echo "  ${YELLOW}./scripts/sync-risingwave.sh --upgrade v3.x.x${NC}"
}

# Perform upgrade
perform_upgrade() {
    local target_version="$1"

    log_info "Upgrading RisingWave to $target_version..."

    cd "$PROJECT_ROOT"

    # Verify target version exists
    if ! git ls-remote --tags "$RISINGWAVE_REMOTE" | grep -q "refs/tags/$target_version$"; then
        error_exit "Version $target_version not found in upstream"
    fi

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        log_warning "You have uncommitted changes."
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            error_exit "Aborted by user"
        fi
    fi

    if [ "$DRY_RUN" = true ]; then
        log_warning "[DRY RUN] Would execute:"
        echo "  git subtree pull --prefix=$SUBTREE_PREFIX $RISINGWAVE_REMOTE $target_version --squash"
        return
    fi

    # Perform subtree pull
    log_info "Pulling changes from upstream (this may take several minutes)..."
    log_warning "You may see merge conflicts - resolve them if they occur"

    if git subtree pull --prefix="$SUBTREE_PREFIX" "$RISINGWAVE_REMOTE" "$target_version" --squash; then
        log_success "RisingWave upgraded to $target_version"

        # Remind about patches
        echo ""
        log_warning "Don't forget to re-apply patches:"
        echo "  ${YELLOW}./scripts/apply-patches.sh${NC}"
        echo ""
        log_warning "And test the build:"
        echo "  ${YELLOW}cargo check --workspace${NC}"

    else
        log_error "Subtree pull failed. You may have merge conflicts."
        echo ""
        echo "To resolve conflicts:"
        echo "  1. Fix conflicts in vendor/risingwave/"
        echo "  2. git add vendor/risingwave/"
        echo "  3. git commit"
        echo ""
        echo "To abort the merge:"
        echo "  git merge --abort"
        exit 1
    fi
}

# Create upgrade branch
create_upgrade_branch() {
    local version="$1"
    local branch_name="feat/risingwave-upgrade-${version//v/}"

    if [ "$DRY_RUN" = true ]; then
        log_info "[DRY RUN] Would create branch: $branch_name"
        return
    fi

    log_info "Creating upgrade branch: $branch_name"

    if git show-ref --verify --quiet "refs/heads/$branch_name"; then
        log_warning "Branch $branch_name already exists"
        read -p "Switch to it? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            git checkout "$branch_name"
        fi
    else
        git checkout -b "$branch_name"
        log_success "Created and switched to branch $branch_name"
    fi
}

# Print upgrade summary
print_upgrade_summary() {
    local old_version="$1"
    local new_version="$2"

    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}RisingWave Upgrade Summary${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "Old version: $old_version"
    echo "New version: $new_version"
    echo ""
    echo -e "${BLUE}Next Steps:${NC}"
    echo ""
    echo "1. Re-apply patches:"
    echo "   ${YELLOW}./scripts/apply-patches.sh${NC}"
    echo ""
    echo "2. Resolve any patch conflicts if they occur"
    echo ""
    echo "3. Update dependencies if needed:"
    echo "   ${YELLOW}cargo update${NC}"
    echo ""
    echo "4. Test compilation:"
    echo "   ${YELLOW}cargo check --workspace --all-features${NC}"
    echo ""
    echo "5. Run tests:"
    echo "   ${YELLOW}cargo test --workspace${NC}"
    echo ""
    echo "6. If all tests pass, commit:"
    echo "   ${YELLOW}git commit -m 'chore(risingwave): upgrade to $new_version'${NC}"
    echo ""
    echo "7. Create PR and run full CI"
    echo ""
}

# Main function
main() {
    parse_args "$@"

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}RisingWave Sync Tool${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    check_prerequisites

    if [ "$CHECK_ONLY" = true ]; then
        check_updates
        exit 0
    fi

    if [ -z "$UPGRADE_VERSION" ]; then
        log_error "No action specified"
        echo ""
        echo "Usage:"
        echo "  Check for updates:  ./scripts/sync-risingwave.sh --check"
        echo "  Upgrade to version: ./scripts/sync-risingwave.sh --upgrade v3.1.0"
        echo ""
        exit 1
    fi

    # Perform upgrade
    local current_version
    current_version=$(get_current_version)

    if [ "$current_version" = "${UPGRADE_VERSION//v/}" ]; then
        log_warning "Already at version $UPGRADE_VERSION"
        exit 0
    fi

    create_upgrade_branch "$UPGRADE_VERSION"
    perform_upgrade "$UPGRADE_VERSION"

    if [ "$DRY_RUN" = false ]; then
        print_upgrade_summary "$current_version" "$UPGRADE_VERSION"
    fi
}

main "$@"
