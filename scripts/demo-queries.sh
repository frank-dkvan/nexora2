#!/bin/bash
# ========================================
# Nexora 航空货运站演示 - 快速查询脚本
# ========================================

API_BASE="http://127.0.0.1:8080"

# 检查服务器
if ! curl -s "${API_BASE}/health" > /dev/null 2>&1; then
    echo "❌ 错误：Nexora 服务器未运行"
    exit 1
fi

# 执行查询并美化输出
query() {
    local title="$1"
    local cypher="$2"

    echo ""
    echo "========================================="
    echo "$title"
    echo "========================================="
    echo ""
    echo "查询语句:"
    echo "$cypher" | sed 's/^/   /'
    echo ""
    echo "查询结果:"
    echo ""

    response=$(curl -s -X POST "${API_BASE}/api/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\": $(echo "$cypher" | jq -Rs .)}")

    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        echo "❌ 查询失败:"
        echo "$response" | jq -r '.error' | sed 's/^/   /'
    else
        echo "$response" | jq -r '.results[] | to_entries | map("\(.key): \(.value)") | join(", ")' | sed 's/^/   /'
    fi

    echo ""
}

echo "========================================="
echo "🛫 Nexora 航空货运站演示查询"
echo "========================================="

# 查询1：所有机场
query "查询1：查看所有机场" \
"MATCH (a:Airport)
RETURN a.code AS 代码, a.name AS 名称, a.city AS 城市, a.capacity_tons_per_day AS 日处理能力吨
ORDER BY a.capacity_tons_per_day DESC"

# 查询2：上海到洛杉矶航线
query "查询2：上海浦东到洛杉矶的航线" \
"MATCH (origin:Airport {code: 'PVG'})-[r:ROUTE]->(dest:Airport {code: 'LAX'})
RETURN r.airline AS 航空公司, r.flight_number AS 航班号, r.frequency_per_week AS 每周班次, r.flight_time_hours AS 飞行时间小时"

# 查询3：在途货物
query "查询3：所有在途货物" \
"MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE s.status = '在途'
RETURN s.awb_number AS 运单号, s.origin AS 起运地, s.destination AS 目的地, ct.category AS 货物类型, s.weight_kg AS 重量千克"

# 查询4：冷链货物
query "查询4：需要冷链运输的货物" \
"MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE ct.requires_temperature_control = true
RETURN s.awb_number AS 运单号, ct.category AS 货物类型, ct.temp_range_celsius AS 温度要求, s.status AS 状态"

# 查询5：浦东货运站库存
query "查询5：浦东机场货运站当前库存" \
"MATCH (s:Shipment)-[r:STORED_AT]->(t:CargoTerminal {terminal_id: 'PVG-T1'})
RETURN s.awb_number AS 运单号, s.weight_kg AS 重量千克, r.warehouse_location AS 仓位, s.status AS 状态
ORDER BY r.check_in_time DESC"

# 查询6：高价值货物
query "查询6：高价值货物（>10万美元）" \
"MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE s.declared_value_usd > 100000
RETURN s.awb_number AS 运单号, ct.category AS 货物类型, s.declared_value_usd AS 价值美元, s.status AS 状态
ORDER BY s.declared_value_usd DESC"

# 查询7：货物类型统计
query "查询7：货物类型分布统计" \
"MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
RETURN ct.category AS 货物类型, count(s) AS 运单数量, sum(s.weight_kg) AS 总重量千克, sum(s.declared_value_usd) AS 总价值美元
ORDER BY 总价值美元 DESC"

# 查询8：危险品货运站
query "查询8：具备危险品处理资质的货运站" \
"MATCH (a:Airport)-[:HAS_TERMINAL]->(t:CargoTerminal)
WHERE t.dangerous_goods_certified = true
RETURN a.code AS 机场代码, a.name AS 机场名称, t.terminal_id AS 货运站ID, t.warehouse_capacity_tons AS 容量吨
ORDER BY t.warehouse_capacity_tons DESC"

echo "========================================="
echo "✨ 演示查询完成！"
echo "========================================="
echo ""
echo "💡 提示："
echo "   - 查看更多查询示例: examples/air-cargo-queries.cypher"
echo "   - API 文档: http://127.0.0.1:8080/api/docs"
echo "   - 健康检查: curl http://127.0.0.1:8080/health"
echo ""
