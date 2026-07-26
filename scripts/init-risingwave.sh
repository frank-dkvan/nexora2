#!/usr/bin/env bash
# init-risingwave.sh - Initialize RisingWave as Git Subtree in Nexora 2
#
# This script adds RisingWave v3.0.2 as a Git Subtree under vendor/risingwave/
# and sets up the necessary directory structure for integration.
#
# Usage:
#   ./scripts/init-risingwave.sh [--version VERSION]
#
# Options:
#   --version VERSION    RisingWave version to integrate (default: v3.0.2)
#
# Prerequisites:
#   - Git 2.9+ (for subtree support)
#   - Clean working directory (no uncommitted changes)
#
# What this script does:
#   1. Validates prerequisites
#   2. Adds RisingWave remote
#   3. Adds RisingWave as Git Subtree (squashed)
#   4. Creates directory structure for integration crates
#   5. Creates placeholder Cargo.toml files
#   6. Updates .gitignore
#   7. Commits the changes

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
RISINGWAVE_VERSION="${1:-v3.0.2}"
RISINGWAVE_REMOTE="risingwave-upstream"
RISINGWAVE_REPO="https://github.com/risingwavelabs/risingwave.git"
SUBTREE_PREFIX="vendor/risingwave"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            RISINGWAVE_VERSION="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '2,17p' "$0" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            echo "Run with --help for usage information"
            exit 1
            ;;
    esac
done

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Error handler
error_exit() {
    log_error "$1"
    exit 1
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        error_exit "Not a git repository. Please run from project root."
    fi

    # Check if we're in the project root
    if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
        error_exit "Cargo.toml not found. Please run from project root."
    fi

    # Check git version
    local git_version
    git_version=$(git --version | awk '{print $3}')
    local required_version="2.9.0"

    if ! printf '%s\n%s\n' "$required_version" "$git_version" | sort -V -C; then
        error_exit "Git version $required_version or higher required (found $git_version)"
    fi

    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        log_warning "You have uncommitted changes."
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            error_exit "Aborted by user. Please commit or stash your changes first."
        fi
    fi

    # Check if vendor/risingwave already exists
    if [ -d "$PROJECT_ROOT/$SUBTREE_PREFIX" ]; then
        log_warning "vendor/risingwave already exists."
        read -p "Remove and re-initialize? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "Removing existing vendor/risingwave..."
            rm -rf "$PROJECT_ROOT/$SUBTREE_PREFIX"
            git add "$PROJECT_ROOT/$SUBTREE_PREFIX"
            git commit -m "chore(risingwave): remove existing subtree for re-initialization" || true
        else
            error_exit "Aborted by user. vendor/risingwave already exists."
        fi
    fi

    log_success "Prerequisites check passed"
}

# Add RisingWave remote
add_risingwave_remote() {
    log_info "Adding RisingWave remote..."

    # Check if remote already exists
    if git remote get-url "$RISINGWAVE_REMOTE" > /dev/null 2>&1; then
        log_warning "Remote '$RISINGWAVE_REMOTE' already exists. Updating URL..."
        git remote set-url "$RISINGWAVE_REMOTE" "$RISINGWAVE_REPO"
    else
        git remote add "$RISINGWAVE_REMOTE" "$RISINGWAVE_REPO"
    fi

    log_success "RisingWave remote added: $RISINGWAVE_REPO"
}

# Fetch RisingWave repository
fetch_risingwave() {
    log_info "Fetching RisingWave repository (this may take a few minutes)..."

    # Fetch with depth=1 to save bandwidth (we only need the specific version)
    if ! git fetch "$RISINGWAVE_REMOTE" "$RISINGWAVE_VERSION" --depth=1; then
        error_exit "Failed to fetch RisingWave version $RISINGWAVE_VERSION"
    fi

    log_success "Fetched RisingWave $RISINGWAVE_VERSION"
}

# Add RisingWave as Git Subtree
add_subtree() {
    log_info "Adding RisingWave as Git Subtree (squashed)..."
    log_warning "This may take 5-10 minutes due to RisingWave's size..."

    # Add subtree with --squash to keep history clean
    if ! git subtree add --prefix="$SUBTREE_PREFIX" "$RISINGWAVE_REMOTE" "$RISINGWAVE_VERSION" --squash; then
        error_exit "Failed to add Git Subtree"
    fi

    log_success "RisingWave added as Git Subtree at $SUBTREE_PREFIX"
}

# Create directory structure
create_directory_structure() {
    log_info "Creating directory structure for RisingWave integration..."

    cd "$PROJECT_ROOT"

    # Create new crates
    mkdir -p crates/nexora-risingwave/src
    mkdir -p crates/nexora-consensus/src
    mkdir -p crates/nexora-rpc/src

    # Create extensions directory
    mkdir -p extensions/meta_raft/src

    # Create patches directory
    mkdir -p patches

    log_success "Directory structure created"
}

# Create placeholder Cargo.toml files
create_placeholder_crates() {
    log_info "Creating placeholder Cargo.toml files..."

    cd "$PROJECT_ROOT"

    # nexora-risingwave
    cat > crates/nexora-risingwave/Cargo.toml <<'EOF'
[package]
name = "nexora-risingwave"
description = "RisingWave integration wrapper for Nexora 2"
version.workspace = true
edition.workspace = true

[dependencies]
nexora-core = { path = "../nexora-core" }
nexora-consensus = { path = "../nexora-consensus", optional = true }
nexora-rpc = { path = "../nexora-rpc", optional = true }
tokio = { workspace = true }
anyhow = { workspace = true }
tracing = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }

# RisingWave dependencies (will be added in Phase 3)
# risingwave_meta = { path = "../../vendor/risingwave/src/meta" }
# risingwave_frontend = { path = "../../vendor/risingwave/src/frontend" }

[features]
default = []
raft-ha = ["dep:nexora-consensus", "dep:nexora-rpc"]

[dev-dependencies]
tokio = { workspace = true }
EOF

    # nexora-consensus
    cat > crates/nexora-consensus/Cargo.toml <<'EOF'
[package]
name = "nexora-consensus"
description = "Consensus protocol abstraction (Raft-based)"
version.workspace = true
edition.workspace = true

[dependencies]
tokio = { workspace = true }
async-trait = "0.1"
bytes = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

# Raft implementation (will be added in Phase 2)
# openraft = "0.9"

[dev-dependencies]
tokio = { workspace = true }
EOF

    # nexora-rpc
    cat > crates/nexora-rpc/Cargo.toml <<'EOF'
[package]
name = "nexora-rpc"
description = "RPC abstraction layer (gRPC-based)"
version.workspace = true
edition.workspace = true

[dependencies]
tokio = { workspace = true }
async-trait = "0.1"
bytes = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

# gRPC implementation (will be added in Phase 2)
# tonic = "0.11"
# prost = "0.12"

[dev-dependencies]
tokio = { workspace = true }
EOF

    # extensions/meta_raft
    cat > extensions/meta_raft/Cargo.toml <<'EOF'
[package]
name = "extensions-meta-raft"
description = "RisingWave Meta Raft HA extension"
version.workspace = true
edition.workspace = true

[dependencies]
nexora-consensus = { path = "../../crates/nexora-consensus" }
nexora-rpc = { path = "../../crates/nexora-rpc" }
tokio = { workspace = true }
async-trait = "0.1"
anyhow = { workspace = true }
tracing = { workspace = true }

# RisingWave Meta dependencies (will be added in Phase 4)
# risingwave_meta = { path = "../../vendor/risingwave/src/meta" }

[dev-dependencies]
tokio = { workspace = true }
EOF

    # Create placeholder lib.rs files
    echo "// Placeholder - to be implemented in Phase 2" > crates/nexora-consensus/src/lib.rs
    echo "// Placeholder - to be implemented in Phase 2" > crates/nexora-rpc/src/lib.rs
    echo "// Placeholder - to be implemented in Phase 3" > crates/nexora-risingwave/src/lib.rs
    echo "// Placeholder - to be implemented in Phase 4" > extensions/meta_raft/src/lib.rs

    # Create patches README
    cat > patches/README.md <<'EOF'
# RisingWave Patches

This directory contains minimal patches to enable RisingWave integration with Nexora 2.

## Patch List

- `001-enable-external-election.patch` - Enable external election plugin (Phase 4)
- `002-expose-election-trait.patch` - Expose ElectionClient trait (Phase 4)

## Applying Patches

```bash
# Apply all patches
../scripts/apply-patches.sh

# Apply single patch manually
git apply patches/001-enable-external-election.patch
```

## Creating New Patches

```bash
# Make changes in vendor/risingwave/
cd vendor/risingwave
# ... edit files ...

# Create patch
git diff > ../../patches/003-my-change.patch
```

## Patch Guidelines

- Keep patches minimal (<100 lines each)
- One logical change per patch
- Document why the patch is needed
- Test that patches apply cleanly after RisingWave upgrades
EOF

    log_success "Placeholder crates created"
}

# Update root Cargo.toml
update_cargo_toml() {
    log_info "Updating root Cargo.toml..."

    cd "$PROJECT_ROOT"

    # Check if new members already exist
    if grep -q "nexora-risingwave" Cargo.toml; then
        log_warning "Cargo.toml already contains new workspace members, skipping"
        return
    fi

    # Create backup
    cp Cargo.toml Cargo.toml.backup

    # Add new workspace members (insert before the closing bracket)
    # This is a simple append - manual adjustment may be needed
    cat >> Cargo.toml <<'EOF'

    # RisingWave Integration (Phase 1)
    "crates/nexora-risingwave",
    "crates/nexora-consensus",
    "crates/nexora-rpc",
    "extensions/meta_raft",
EOF

    log_warning "Added new workspace members to Cargo.toml"
    log_warning "Please manually verify the Cargo.toml format is correct"
}

# Update .gitignore
update_gitignore() {
    log_info "Updating .gitignore..."

    cd "$PROJECT_ROOT"

    # Check if already updated
    if grep -q "vendor/risingwave/target" .gitignore; then
        log_warning ".gitignore already contains RisingWave entries, skipping"
        return
    fi

    # Append RisingWave-specific ignores
    cat >> .gitignore <<'EOF'

# RisingWave Integration
/vendor/risingwave/target/
/vendor/risingwave/.idea/
/vendor/risingwave/.vscode/
/vendor/risingwave/Cargo.lock
/vendor/risingwave/.DS_Store
EOF

    log_success ".gitignore updated"
}

# Verify integration
verify_integration() {
    log_info "Verifying integration..."

    cd "$PROJECT_ROOT"

    # Check that vendor/risingwave exists
    if [ ! -d "$SUBTREE_PREFIX" ]; then
        error_exit "vendor/risingwave directory not found"
    fi

    # Check that RisingWave Cargo.toml exists
    if [ ! -f "$SUBTREE_PREFIX/Cargo.toml" ]; then
        error_exit "vendor/risingwave/Cargo.toml not found"
    fi

    # Check new crates exist
    local crates=(
        "crates/nexora-risingwave"
        "crates/nexora-consensus"
        "crates/nexora-rpc"
        "extensions/meta_raft"
    )

    for crate in "${crates[@]}"; do
        if [ ! -f "$crate/Cargo.toml" ]; then
            error_exit "$crate/Cargo.toml not found"
        fi
    done

    log_success "Integration verified"
}

# Commit changes
commit_changes() {
    log_info "Committing changes..."

    cd "$PROJECT_ROOT"

    # Stage new directories and files
    git add crates/nexora-risingwave
    git add crates/nexora-consensus
    git add crates/nexora-rpc
    git add extensions/meta_raft
    git add patches
    git add .gitignore

    # Commit (subtree add already created a commit, this is for our scaffolding)
    if git diff --cached --quiet; then
        log_warning "No changes to commit (subtree commit already created)"
    else
        git commit -m "feat(risingwave): Phase 1 - add integration scaffolding

- Created nexora-risingwave wrapper crate
- Created nexora-consensus abstraction crate
- Created nexora-rpc abstraction crate
- Created extensions/meta_raft crate
- Created patches/ directory
- Updated .gitignore for RisingWave files

Part of RisingWave Integration Plan Phase 1"
    fi

    log_success "Changes committed"
}

# Print next steps
print_next_steps() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}RisingWave Integration - Phase 1 Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "RisingWave $RISINGWAVE_VERSION has been successfully integrated as a Git Subtree."
    echo ""
    echo -e "${BLUE}Next Steps:${NC}"
    echo ""
    echo "1. Verify Cargo.toml workspace members:"
    echo "   ${YELLOW}vim Cargo.toml${NC}"
    echo "   (Ensure the new members are properly formatted)"
    echo ""
    echo "2. Test that existing code still compiles:"
    echo "   ${YELLOW}cargo check --workspace${NC}"
    echo ""
    echo "3. Run existing tests to ensure nothing broke:"
    echo "   ${YELLOW}cargo test --workspace${NC}"
    echo ""
    echo "4. Review the integration:"
    echo "   ${YELLOW}ls -la vendor/risingwave/${NC}"
    echo "   ${YELLOW}ls -la crates/nexora-risingwave/${NC}"
    echo "   ${YELLOW}ls -la crates/nexora-consensus/${NC}"
    echo ""
    echo "5. Create sync script:"
    echo "   Next, implement ${YELLOW}scripts/sync-risingwave.sh${NC}"
    echo ""
    echo "6. Proceed to Phase 2:"
    echo "   Develop nexora-consensus and nexora-rpc abstractions"
    echo ""
    echo -e "${BLUE}Documentation:${NC}"
    echo "  - Integration Plan: docs/RISINGWAVE_INTEGRATION_PLAN.md"
    echo "  - Development Guide: CLAUDE.md"
    echo ""
    echo -e "${BLUE}Useful Commands:${NC}"
    echo "  - Sync with upstream: ${YELLOW}./scripts/sync-risingwave.sh --upgrade v3.1.0${NC}"
    echo "  - Apply patches: ${YELLOW}./scripts/apply-patches.sh${NC}"
    echo "  - Build with RisingWave: ${YELLOW}cargo build --features risingwave${NC}"
    echo ""
}

# Main execution
main() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Nexora 2 - RisingWave Integration${NC}"
    echo -e "${BLUE}Phase 1: Repository Setup${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo "This script will:"
    echo "  1. Add RisingWave $RISINGWAVE_VERSION as Git Subtree"
    echo "  2. Create integration crates structure"
    echo "  3. Update Cargo.toml and .gitignore"
    echo ""
    read -p "Continue? (Y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        log_warning "Aborted by user"
        exit 0
    fi

    check_prerequisites
    add_risingwave_remote
    fetch_risingwave
    add_subtree
    create_directory_structure
    create_placeholder_crates
    update_cargo_toml
    update_gitignore
    verify_integration
    commit_changes
    print_next_steps
}

# Run main function
main "$@"
