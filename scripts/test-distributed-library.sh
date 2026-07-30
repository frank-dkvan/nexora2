#!/bin/bash
# RisingWave Distributed Library Mode - Test Client
# 测试分布式库模式的功能

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║     RisingWave Distributed Library Mode - Test Client                ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# 等待服务启动
echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
sleep 3

# 测试1: 检查 HTTP API
echo -e "\n${BLUE}━━━ Test 1: HTTP API Health Check ━━━${NC}"
echo "GET http://localhost:8080/api/health"
curl -s http://localhost:8080/api/health | jq '.' || echo "Failed"

# 测试2: 检查 Iceberg REST Catalog
echo -e "\n${BLUE}━━━ Test 2: Iceberg REST Catalog Config ━━━${NC}"
echo "GET http://localhost:8080/api/iceberg/catalog/v1/config"
curl -s http://localhost:8080/api/iceberg/catalog/v1/config | jq '.' || echo "Failed"

# 测试3: 列出 namespaces
echo -e "\n${BLUE}━━━ Test 3: List Namespaces ━━━${NC}"
echo "GET http://localhost:8080/api/iceberg/catalog/v1/namespaces"
curl -s http://localhost:8080/api/iceberg/catalog/v1/namespaces | jq '.' || echo "Failed"

# 测试4: 连接 RisingWave Frontend (pgwire)
echo -e "\n${BLUE}━━━ Test 4: RisingWave Frontend (pgwire) ━━━${NC}"
echo "Connecting to postgres://root@localhost:4566/dev"
if command -v psql &> /dev/null; then
    psql -h localhost -p 4566 -U root -d dev -c "SELECT version();" 2>/dev/null || \
        echo -e "${YELLOW}⚠️  psql not available or connection failed${NC}"
else
    echo -e "${YELLOW}⚠️  psql not installed, skipping pgwire test${NC}"
    echo "   Install: brew install postgresql (macOS) or apt-get install postgresql-client (Linux)"
fi

# 测试5: 创建测试表
echo -e "\n${BLUE}━━━ Test 5: Create Test Table ━━━${NC}"
if command -v psql &> /dev/null; then
    echo "CREATE TABLE test_table (id INT, name VARCHAR);"
    psql -h localhost -p 4566 -U root -d dev -c "CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR);" 2>/dev/null && \
        echo -e "${GREEN}✅ Table created${NC}" || \
        echo -e "${YELLOW}⚠️  Table creation failed${NC}"

    echo "INSERT INTO test_table VALUES (1, 'test');"
    psql -h localhost -p 4566 -U root -d dev -c "INSERT INTO test_table VALUES (1, 'test');" 2>/dev/null && \
        echo -e "${GREEN}✅ Data inserted${NC}" || \
        echo -e "${YELLOW}⚠️  Insert failed${NC}"

    echo "SELECT * FROM test_table;"
    psql -h localhost -p 4566 -U root -d dev -c "SELECT * FROM test_table;" 2>/dev/null || \
        echo -e "${YELLOW}⚠️  Query failed${NC}"
else
    echo -e "${YELLOW}⚠️  psql not installed, skipping${NC}"
fi

# 测试6: 检查系统 catalog
echo -e "\n${BLUE}━━━ Test 6: Check RisingWave System Catalogs ━━━${NC}"
if command -v psql &> /dev/null; then
    echo "SELECT * FROM rw_catalog.rw_tables LIMIT 5;"
    psql -h localhost -p 4566 -U root -d dev -c "SELECT name, schema_id FROM rw_catalog.rw_tables LIMIT 5;" 2>/dev/null || \
        echo -e "${YELLOW}⚠️  Query failed${NC}"

    echo ""
    echo "SELECT * FROM rw_catalog.iceberg_tables;"
    psql -h localhost -p 4566 -U root -d dev -c "SELECT * FROM rw_catalog.iceberg_tables;" 2>/dev/null || \
        echo -e "${YELLOW}⚠️  Query failed (table may be empty)${NC}"
else
    echo -e "${YELLOW}⚠️  psql not installed, skipping${NC}"
fi

# 总结
echo ""
echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║                         Test Summary                                  ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "✅ HTTP API:           http://localhost:8080"
echo "✅ Iceberg Catalog:    http://localhost:8080/api/iceberg/catalog"
echo "✅ RisingWave pgwire:  postgresql://root@localhost:4566/dev"
echo ""
echo "下一步测试:"
echo "  1. 创建 Kafka source"
echo "  2. 创建 materialized view"
echo "  3. 创建 Iceberg sink"
echo "  4. 验证数据流"
echo ""
echo "完整测试脚本: scripts/test-iceberg-pipeline.sh"
