#!/bin/bash
# ========================================
# Nexora 航空货运站演示 - 数据导入脚本
# ========================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
API_BASE="http://127.0.0.1:8080"
DATA_FILE="${PROJECT_ROOT}/examples/air-cargo-demo.cypher"
QUERIES_FILE="${PROJECT_ROOT}/examples/air-cargo-queries.cypher"

echo "========================================="
echo "Nexora 航空货运站数据导入"
echo "========================================="
echo ""

# 检查服务器是否运行
if ! curl -s "${API_BASE}/health" > /dev/null 2>&1; then
    echo "❌ 错误：Nexora 服务器未运行"
    echo "请先运行: ./scripts/start-demo.sh"
    exit 1
fi

echo "✅ 服务器连接成功"
echo ""

# 检查数据文件
if [ ! -f "$DATA_FILE" ]; then
    echo "❌ 错误：未找到数据文件 $DATA_FILE"
    exit 1
fi

echo "📂 数据文件: $DATA_FILE"
echo "📊 查询文件: $QUERIES_FILE"
echo ""
echo "🔄 开始导入数据..."
echo ""

# 函数：执行单个 Cypher 语句
execute_cypher() {
    local query="$1"
    local description="$2"

    echo -n "   执行: $description ... "

    response=$(curl -s -X POST "${API_BASE}/api/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": $(echo "$query" | jq -Rs .)}" 2>&1)

    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        error_msg=$(echo "$response" | jq -r '.error')
        echo "❌ 失败"
        echo "      错误: $error_msg"
        return 1
    else
        echo "✅ 成功"
        return 0
    fi
}

# 读取数据文件并逐语句执行
current_statement=""
line_num=0
success_count=0
fail_count=0

while IFS= read -r line || [ -n "$line" ]; do
    line_num=$((line_num + 1))

    # 跳过空行和注释
    if [[ -z "$line" ]] || [[ "$line" =~ ^[[:space:]]*// ]]; then
        continue
    fi

    # 累积语句
    current_statement+="$line "

    # 检查是否是语句结束（以分号结尾）
    if [[ "$line" =~ \;[[:space:]]*$ ]]; then
        # 清理语句
        statement=$(echo "$current_statement" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

        if [ -n "$statement" ]; then
            # 提取描述（从注释或CREATE关键字）
            if [[ "$statement" =~ CREATE[[:space:]]+\(([^:]+) ]]; then
                entity="${BASH_REMATCH[1]}"
                description="创建 $entity"
            elif [[ "$statement" =~ MATCH.*CREATE[[:space:]]+\([^)]+\)-\[:([^\]]+)\] ]]; then
                rel="${BASH_REMATCH[1]}"
                description="创建关系 $rel"
            else
                description="执行语句"
            fi

            if execute_cypher "$statement" "$description"; then
                success_count=$((success_count + 1))
            else
                fail_count=$((fail_count + 1))
            fi

            # 稍微延迟避免过载
            sleep 0.1
        fi

        current_statement=""
    fi
done < "$DATA_FILE"

echo ""
echo "========================================="
echo "导入完成"
echo "========================================="
echo "✅ 成功: $success_count 条语句"
if [ $fail_count -gt 0 ]; then
    echo "❌ 失败: $fail_count 条语句"
fi
echo ""

# 显示统计信息
echo "📊 数据统计："
echo ""

# 统计节点数量
for label in Airport CargoTerminal Airline CargoType Shipment; do
    count=$(curl -s -X POST "${API_BASE}/api/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"MATCH (n:${label}) RETURN count(n) AS count\"}" \
        | jq -r '.results[0].count // 0' 2>/dev/null || echo "0")
    echo "   ${label}: ${count} 个节点"
done

echo ""

# 统计关系数量
for rel_type in HAS_TERMINAL ROUTE STORED_AT IS_TYPE; do
    count=$(curl -s -X POST "${API_BASE}/api/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"MATCH ()-[r:${rel_type}]->() RETURN count(r) AS count\"}" \
        | jq -r '.results[0].count // 0' 2>/dev/null || echo "0")
    echo "   ${rel_type}: ${count} 个关系"
done

echo ""
echo "========================================="
echo "✨ 数据导入完成！"
echo "========================================="
echo ""
echo "📖 接下来您可以："
echo "   1. 使用 examples/air-cargo-queries.cypher 中的查询进行测试"
echo "   2. 通过 API 执行自定义查询："
echo "      curl -X POST http://127.0.0.1:8080/api/query \\"
echo "           -H 'Content-Type: application/json' \\"
echo "           -d '{\"query\": \"MATCH (a:Airport) RETURN a LIMIT 5\"}'"
echo ""
echo "   3. 或使用我们提供的快速查询脚本："
echo "      ./scripts/demo-queries.sh"
echo ""
