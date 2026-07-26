#!/usr/bin/env bash
# Nexora 2.0 文档一致性验证脚本
# 用途：自动验证文档中的 API 端点、CLI 参数与代码实现一致性

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

ERRORS=0
WARNINGS=0

echo "=================================================="
echo "  Nexora 2.0 Documentation Consistency Checker"
echo "=================================================="
echo ""

# 1. 提取代码中的实际 API 端点
echo "📡 [1/5] Extracting API endpoints from code..."

MAIN_RS="$PROJECT_ROOT/crates/nexora-app/src/main.rs"

if [[ ! -f "$MAIN_RS" ]]; then
    echo -e "${RED}❌ Error: Cannot find main.rs${NC}"
    exit 1
fi

# 提取所有 .route() 定义 (macOS 兼容版本，不使用 -P)
ACTUAL_ROUTES=$(grep -o '\.route("[^"]*"' "$MAIN_RS" | sed 's/\.route("//; s/"$//' | sort -u || true)

if [[ -z "$ACTUAL_ROUTES" ]]; then
    echo -e "${RED}❌ Error: No routes found in main.rs${NC}"
    exit 1
fi

echo "   Found $(echo "$ACTUAL_ROUTES" | wc -l) unique routes"

# 2. 扫描文档中提到的 API 端点
echo ""
echo "📚 [2/5] Scanning documented API endpoints..."

DOC_DIRS=(
    "$PROJECT_ROOT/docs"
    "$PROJECT_ROOT"
)

# 排除已知不是 API 路径的模式
EXCLUDED_PATTERNS=(
    "/api/v2/"         # 旧版本路径，应该已全部清理
    "example.com"
    "localhost"
)

DOC_ENDPOINTS_FILE=$(mktemp)

for dir in "${DOC_DIRS[@]}"; do
    if [[ -d "$dir" ]]; then
        # 查找所有 /api/ 开头的路径 (macOS 兼容版本)
        grep -rho '/api/[a-zA-Z0-9/_\-{}]*' "$dir" --include="*.md" 2>/dev/null | \
            grep -E '^/api/[a-zA-Z0-9/_\-{}]+' >> "$DOC_ENDPOINTS_FILE" || true
    fi
done

# 去重并排序
DOCUMENTED_ROUTES=$(cat "$DOC_ENDPOINTS_FILE" | sort -u || true)
rm -f "$DOC_ENDPOINTS_FILE"

echo "   Found $(echo "$DOCUMENTED_ROUTES" | wc -l) documented endpoints"

# 3. 检查文档中是否还有 /api/v2/ 路径
echo ""
echo "🔍 [3/5] Checking for deprecated /api/v2/ paths..."

V2_PATHS=$(grep -rn "api/v2" "$PROJECT_ROOT" --include="*.md" 2>/dev/null | grep -v ".git" || true)

if [[ -n "$V2_PATHS" ]]; then
    echo -e "${RED}❌ Found deprecated /api/v2/ paths:${NC}"
    echo "$V2_PATHS" | while IFS= read -r line; do
        echo "   $line"
    done
    ((ERRORS++))
else
    echo -e "${GREEN}✅ No /api/v2/ paths found${NC}"
fi

# 4. 验证文档端点是否存在于代码中
echo ""
echo "🔗 [4/5] Validating documented endpoints against code..."

MISSING_ENDPOINTS=()

while IFS= read -r doc_route; do
    # 跳过空行和特殊路径
    [[ -z "$doc_route" ]] && continue
    [[ "$doc_route" == "/api/health" ]] && continue  # 特殊处理
    [[ "$doc_route" == "/api/metrics" ]] && continue

    # 标准化路径参数 (将 {id} 转为 :id 或反过来)
    normalized_route=$(echo "$doc_route" | sed 's/{[^}]*}/:id/g')

    # 检查是否存在于实际路由中
    found=false
    while IFS= read -r actual_route; do
        normalized_actual=$(echo "$actual_route" | sed 's/:[^/]*/:{id}/g')
        normalized_doc=$(echo "$normalized_route" | sed 's/:[^/]*/:{id}/g')

        if [[ "$normalized_actual" == "$normalized_doc" ]]; then
            found=true
            break
        fi
    done <<< "$ACTUAL_ROUTES"

    if [[ "$found" == false ]]; then
        # 再检查是否是 RESTful 别名 (单复数)
        singular=$(echo "$doc_route" | sed 's/ies$/y/; s/s$//')
        plural=$(echo "$doc_route" | sed 's/y$/ies/; s/$/s/')

        found_alias=false
        for variant in "$singular" "$plural"; do
            normalized_variant=$(echo "$variant" | sed 's/{[^}]*}/:id/g')
            while IFS= read -r actual_route; do
                normalized_actual=$(echo "$actual_route" | sed 's/:[^/]*/:{id}/g')
                if [[ "$normalized_actual" == "$normalized_variant" ]]; then
                    found_alias=true
                    break 2
                fi
            done <<< "$ACTUAL_ROUTES"
        done

        if [[ "$found_alias" == false ]]; then
            MISSING_ENDPOINTS+=("$doc_route")
        fi
    fi
done <<< "$DOCUMENTED_ROUTES"

if [[ ${#MISSING_ENDPOINTS[@]} -gt 0 ]]; then
    echo -e "${RED}❌ Found ${#MISSING_ENDPOINTS[@]} documented endpoints not in code:${NC}"
    for endpoint in "${MISSING_ENDPOINTS[@]}"; do
        echo "   $endpoint"
    done
    ((ERRORS++))
else
    echo -e "${GREEN}✅ All documented endpoints exist in code${NC}"
fi

# 5. 验证 CLI 参数
echo ""
echo "⚙️  [5/5] Validating CLI parameters..."

# 提取代码中的 CLI 参数 (macOS 兼容版本)
CLI_PARAMS=$(grep '#\[arg(long' "$MAIN_RS" -A 1 | grep -E '^[[:space:]]*[a-z_]+:' | sed 's/^[[:space:]]*//; s/:.*$//' | sort -u || true)

# 查找文档中使用的 CLI 参数 (macOS 兼容版本)
DOC_CLI_PARAMS=$(grep -rho -- '--[a-z][-a-z0-9]*' "$PROJECT_ROOT" --include="*.md" | grep -v ".git" | sort -u | sed 's/^--//' || true)

echo "   Code defines $(echo "$CLI_PARAMS" | wc -l) CLI parameters"
echo "   Docs mention $(echo "$DOC_CLI_PARAMS" | wc -l) CLI parameters"

# 检查文档中提到但代码不存在的参数
INVALID_CLI_PARAMS=()

while IFS= read -r doc_param; do
    [[ -z "$doc_param" ]] && continue

    # 转换 - 为 _
    normalized_param=$(echo "$doc_param" | tr '-' '_')

    if ! echo "$CLI_PARAMS" | grep -q "^${normalized_param}$"; then
        # 检查是否是常见的环境变量
        if [[ ! "$doc_param" =~ ^(AWS|RUST|CARGO|PATH|HOME) ]]; then
            INVALID_CLI_PARAMS+=("--$doc_param")
        fi
    fi
done <<< "$DOC_CLI_PARAMS"

if [[ ${#INVALID_CLI_PARAMS[@]} -gt 0 ]]; then
    echo -e "${YELLOW}⚠️  Found ${#INVALID_CLI_PARAMS[@]} CLI params in docs not in code (may be false positives):${NC}"
    for param in "${INVALID_CLI_PARAMS[@]}"; do
        echo "   $param"
    done
    ((WARNINGS++))
else
    echo -e "${GREEN}✅ All documented CLI parameters are valid${NC}"
fi

# 总结
echo ""
echo "=================================================="
echo "                   SUMMARY"
echo "=================================================="
echo ""

if [[ $ERRORS -eq 0 ]] && [[ $WARNINGS -eq 0 ]]; then
    echo -e "${GREEN}✅ All checks passed!${NC}"
    echo ""
    exit 0
elif [[ $ERRORS -eq 0 ]]; then
    echo -e "${YELLOW}⚠️  Passed with $WARNINGS warning(s)${NC}"
    echo ""
    exit 0
else
    echo -e "${RED}❌ Found $ERRORS error(s) and $WARNINGS warning(s)${NC}"
    echo ""
    echo "Please fix the errors above before committing."
    exit 1
fi
