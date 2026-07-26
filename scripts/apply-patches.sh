#!/usr/bin/env bash
# apply-patches.sh - Apply Nexora-specific patches to RisingWave
#
# This script applies all patches in the patches/ directory to the RisingWave
# subtree. Patches are applied in numerical order.
#
# Usage:
#   ./scripts/apply-patches.sh [OPTIONS]
#
# Options:
#   --check              Check if patches would apply cleanly (dry run)
#   --reverse            Reverse/unapply all patches
#   --verbose            Show detailed output
#   --patch FILE         Apply specific patch only
#   -h, --help          Show this help message
#
# Examples:
#   ./scripts/apply-patches.sh
#   ./scripts/apply-patches.sh --check
#   ./scripts/apply-patches.sh --patch patches/001-enable-external-election.patch

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PATCHES_DIR="$PROJECT_ROOT/patches"
SUBTREE_PREFIX="vendor/risingwave"

# Options
CHECK_ONLY=false
REVERSE=false
VERBOSE=false
SPECIFIC_PATCH=""

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
            --reverse)
                REVERSE=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --patch)
                SPECIFIC_PATCH="$2"
                shift 2
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
        error_exit "RisingWave subtree not found at $SUBTREE_PREFIX"
    fi

    if [ ! -d "$PATCHES_DIR" ]; then
        error_exit "Patches directory not found at $PATCHES_DIR"
    fi
}

# Get list of patches
get_patches() {
    if [ -n "$SPECIFIC_PATCH" ]; then
        if [ ! -f "$SPECIFIC_PATCH" ]; then
            error_exit "Patch file not found: $SPECIFIC_PATCH"
        fi
        echo "$SPECIFIC_PATCH"
    else
        # Find all .patch files, sort numerically
        find "$PATCHES_DIR" -name "*.patch" -type f | sort -V
    fi
}

# Check if patch would apply
check_patch() {
    local patch_file="$1"
    local patch_name
    patch_name=$(basename "$patch_file")

    if [ "$VERBOSE" = true ]; then
        log_info "Checking $patch_name..."
    fi

    cd "$PROJECT_ROOT"

    if git apply --check "$patch_file" 2>/dev/null; then
        if [ "$VERBOSE" = true ]; then
            log_success "$patch_name would apply cleanly"
        fi
        return 0
    else
        log_warning "$patch_name would NOT apply cleanly"
        return 1
    fi
}

# Apply single patch
apply_patch() {
    local patch_file="$1"
    local patch_name
    patch_name=$(basename "$patch_file")

    log_info "Applying $patch_name..."

    cd "$PROJECT_ROOT"

    # Try to apply
    if [ "$VERBOSE" = true ]; then
        git apply "$patch_file"
    else
        git apply "$patch_file" 2>&1 | grep -v "^$" || true
    fi

    if [ $? -eq 0 ]; then
        log_success "$patch_name applied successfully"
        return 0
    else
        log_error "$patch_name failed to apply"

        # Try with --reject to create .rej files
        log_info "Trying with --reject to identify conflicts..."
        git apply --reject "$patch_file" 2>&1 || true

        echo ""
        log_error "Patch application failed. Conflict details saved in .rej files"
        echo ""
        echo "To resolve manually:"
        echo "  1. Review .rej files in vendor/risingwave/"
        echo "  2. Manually apply changes"
        echo "  3. Remove .rej files"
        echo "  4. Test: cargo check -p risingwave_meta"
        echo "  5. Update patch: git diff > $patch_file"
        echo ""

        return 1
    fi
}

# Reverse single patch
reverse_patch() {
    local patch_file="$1"
    local patch_name
    patch_name=$(basename "$patch_file")

    log_info "Reversing $patch_name..."

    cd "$PROJECT_ROOT"

    if git apply --reverse "$patch_file" 2>/dev/null; then
        log_success "$patch_name reversed successfully"
        return 0
    else
        log_warning "$patch_name could not be reversed (may not be applied)"
        return 1
    fi
}

# Apply all patches
apply_all_patches() {
    local patches
    patches=$(get_patches)

    if [ -z "$patches" ]; then
        log_warning "No patches found in $PATCHES_DIR"
        return 0
    fi

    local total_patches
    total_patches=$(echo "$patches" | wc -l | tr -d ' ')
    local applied=0
    local failed=0

    echo ""
    log_info "Found $total_patches patch(es) to apply"
    echo ""

    for patch_file in $patches; do
        if apply_patch "$patch_file"; then
            ((applied++))
        else
            ((failed++))
        fi
    done

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Patch Application Summary${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo "Total patches: $total_patches"
    echo -e "${GREEN}Applied successfully: $applied${NC}"
    if [ $failed -gt 0 ]; then
        echo -e "${RED}Failed: $failed${NC}"
    fi
    echo ""

    if [ $failed -gt 0 ]; then
        error_exit "Some patches failed to apply. Review errors above."
    else
        log_success "All patches applied successfully"

        # Stage changes
        if [ "$(git diff --name-only $SUBTREE_PREFIX | wc -l)" -gt 0 ]; then
            log_info "Staging patched files..."
            git add "$SUBTREE_PREFIX"
            log_success "Changes staged (use 'git commit' when ready)"
        fi
    fi
}

# Check all patches
check_all_patches() {
    local patches
    patches=$(get_patches)

    if [ -z "$patches" ]; then
        log_warning "No patches found in $PATCHES_DIR"
        return 0
    fi

    local total_patches
    total_patches=$(echo "$patches" | wc -l | tr -d ' ')
    local clean=0
    local conflicts=0

    echo ""
    log_info "Checking $total_patches patch(es)..."
    echo ""

    for patch_file in $patches; do
        if check_patch "$patch_file"; then
            ((clean++))
        else
            ((conflicts++))
        fi
    done

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Patch Check Summary${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo "Total patches: $total_patches"
    echo -e "${GREEN}Would apply cleanly: $clean${NC}"
    if [ $conflicts -gt 0 ]; then
        echo -e "${YELLOW}Would have conflicts: $conflicts${NC}"
    fi
    echo ""

    if [ $conflicts -gt 0 ]; then
        log_warning "Some patches would not apply cleanly"
        echo ""
        echo "This is expected after a RisingWave upgrade."
        echo "You'll need to manually update the patches:"
        echo ""
        echo "  1. Apply with --reject: ./scripts/apply-patches.sh"
        echo "  2. Resolve conflicts in vendor/risingwave/"
        echo "  3. Regenerate patch: git diff vendor/risingwave/ > patches/XXX.patch"
        echo ""
        return 1
    else
        log_success "All patches would apply cleanly"
        return 0
    fi
}

# Reverse all patches
reverse_all_patches() {
    local patches
    # Reverse in opposite order
    patches=$(get_patches | tac)

    if [ -z "$patches" ]; then
        log_warning "No patches found in $PATCHES_DIR"
        return 0
    fi

    local total_patches
    total_patches=$(echo "$patches" | wc -l | tr -d ' ')
    local reversed=0

    echo ""
    log_info "Reversing $total_patches patch(es)..."
    echo ""

    for patch_file in $patches; do
        if reverse_patch "$patch_file"; then
            ((reversed++))
        fi
    done

    echo ""
    log_success "Reversed $reversed patch(es)"

    # Stage changes
    if [ "$(git diff --name-only $SUBTREE_PREFIX | wc -l)" -gt 0 ]; then
        log_info "Staging changes..."
        git add "$SUBTREE_PREFIX"
        log_success "Changes staged"
    fi
}

# List patches
list_patches() {
    local patches
    patches=$(get_patches)

    if [ -z "$patches" ]; then
        log_warning "No patches found in $PATCHES_DIR"
        return
    fi

    echo ""
    echo "Available patches:"
    echo ""

    for patch_file in $patches; do
        local patch_name
        patch_name=$(basename "$patch_file")

        # Extract description from patch header
        local description
        description=$(head -5 "$patch_file" | grep -E "^(Subject:|# )" | sed 's/^Subject: //' | sed 's/^# //' | head -1)

        if [ -n "$description" ]; then
            echo "  - $patch_name: $description"
        else
            echo "  - $patch_name"
        fi
    done

    echo ""
}

# Create patch template
create_patch_template() {
    local next_number
    next_number=$(find "$PATCHES_DIR" -name "*.patch" -type f | wc -l | tr -d ' ')
    next_number=$((next_number + 1))

    local template_file="$PATCHES_DIR/$(printf "%03d" $next_number)-new-patch.patch"

    cat > "$template_file" <<'EOF'
# Subject: Brief description of the patch
#
# This patch modifies RisingWave to enable [feature/fix].
#
# Why this patch is needed:
# - Reason 1
# - Reason 2
#
# Affected files:
# - vendor/risingwave/path/to/file.rs
#
# Testing:
# - How to verify this patch works
#
# Upstream tracking:
# - Related upstream issue/PR (if any)

diff --git a/vendor/risingwave/path/to/file.rs b/vendor/risingwave/path/to/file.rs
index abc123..def456 100644
--- a/vendor/risingwave/path/to/file.rs
+++ b/vendor/risingwave/path/to/file.rs
@@ -10,6 +10,9 @@

 // Add your changes here
+// Example:
+// pub fn new_function() {
+//     println!("Hello from patch!");
+// }

EOF

    log_success "Created patch template: $template_file"
    echo ""
    echo "Next steps:"
    echo "  1. Make changes in vendor/risingwave/"
    echo "  2. Generate diff: git diff vendor/risingwave/ > $template_file"
    echo "  3. Edit patch header with description"
    echo "  4. Test: ./scripts/apply-patches.sh --patch $template_file"
}

# Main function
main() {
    parse_args "$@"

    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}RisingWave Patch Management${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    check_prerequisites

    # List patches if verbose
    if [ "$VERBOSE" = true ]; then
        list_patches
    fi

    if [ "$CHECK_ONLY" = true ]; then
        check_all_patches
        exit $?
    fi

    if [ "$REVERSE" = true ]; then
        reverse_all_patches
        exit 0
    fi

    # Apply patches
    apply_all_patches
}

main "$@"
