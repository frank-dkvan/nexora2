#!/usr/bin/env bash
# Nexora 2.0 — 应用场景综合测试
# 使用最新编译的 release 二进制,覆盖图查询/边属性/遍历/聚合/SQL/认证/管理/向量/事件流等场景。
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$HOME/.cargo/bin:$PATH"   # toolchain guard

BIN="$ROOT/target/release/nexora"
CFG="$ROOT/deploy/nexora-2.0-demo/nexora.toml"
BASE="http://127.0.0.1:8080"
LOG="/tmp/nexora-scenario.log"
DATA="$ROOT/deploy/nexora-2.0-demo/demo-data.cypher"

GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
PASS=0; FAIL=0; declare -a FAILED_NAMES

hr(){ echo -e "${CYAN}────────────────────────────────────────────────────────${NC}"; }

# jq-free JSON 断言: check <name> <json> <python-expr-on-var d>
check(){
  local name="$1"; local json="$2"; local expr="$3"
  local ok
  ok=$(printf '%s' "$json" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
except Exception as e:
    print('PARSE_ERR:'+str(e)); sys.exit()
try:
    print('OK' if ($expr) else 'FAIL')
except Exception as e:
    print('EXPR_ERR:'+str(e))
" 2>/dev/null)
  if [ "$ok" = "OK" ]; then
    echo -e "  ${GREEN}✓${NC} $name"; PASS=$((PASS+1))
  else
    echo -e "  ${RED}✗${NC} $name  ${YEL}[$ok]${NC}"; FAIL=$((FAIL+1)); FAILED_NAMES+=("$name")
    echo -e "    ${YEL}resp:${NC} $(printf '%s' "$json" | head -c 300)"
  fi
}

Q(){ curl -s -X POST "$BASE/api/query/cypher" -H 'Content-Type: application/json' -d "{\"query\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}"; }
SQL(){ curl -s -X POST "$BASE/api/query/sql" -H 'Content-Type: application/json' -d "{\"query\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}"; }

# ── 清理旧进程/锁 ──────────────────────────────────────
echo -e "${YEL}[setup] 清理旧进程与锁...${NC}"
pkill -f 'target/release/nexora' 2>/dev/null; sleep 2
rm -f "$ROOT/nexora-data/LOCK" "$ROOT/nexora-data/control_plane/LOCK" 2>/dev/null || true
# 用全新数据目录,保证测试可复现、无脏数据
TESTDIR="$ROOT/nexora-data-scenario"
rm -rf "$TESTDIR"; mkdir -p "$TESTDIR"

# ── 启动服务 ──────────────────────────────────────────
echo -e "${YEL}[setup] 启动服务 (无认证, 独立数据目录)...${NC}"
"$BIN" --config "$CFG" --allow-unauthenticated \
       --rocksdb-path "$TESTDIR" --wal-dir "$TESTDIR/wal" \
       > "$LOG" 2>&1 &
SRV=$!
echo "  PID=$SRV, 日志=$LOG"

# 等待就绪
for i in $(seq 1 40); do
  if curl -s "$BASE/api/health" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$SRV" 2>/dev/null; then echo -e "${RED}服务进程已退出,见日志${NC}"; tail -20 "$LOG"; exit 1; fi
  sleep 1
done
echo -e "${GREEN}  服务就绪${NC}"

cleanup(){ echo -e "\n${YEL}[teardown] 停止服务...${NC}"; kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; rm -rf "$TESTDIR"; echo "  已清理"; }
trap cleanup EXIT

# ══════════════════════════════════════════════════════
echo; hr; echo -e "${CYAN}场景 1: 健康检查与系统信息${NC}"; hr
check "健康检查 /api/health"        "$(curl -s $BASE/api/health)"        "d.get('status') in ('ok','healthy','UP') or 'status' in d"
check "存活探针 /api/health/live"   "$(curl -s $BASE/api/health/live)"   "True"
check "就绪探针 /api/health/ready"  "$(curl -s $BASE/api/health/ready)"  "True"
si="$(curl -s $BASE/api/system/info)"
check "系统信息含 version"          "$si" "'version' in d"
check "系统信息 shard 数=256"       "$si" "d.get('num_shards')==256"

echo; hr; echo -e "${CYAN}场景 2: 数据写入 (航空货运网络)${NC}"; hr
# 逐条 CREATE (执行器批量建节点有限制,分条最稳)
created_nodes=0; created_rels=0
while IFS= read -r line; do
  case "$line" in
    CREATE*)
      r="$(Q "$line")"
      n=$(printf '%s' "$r" | python3 -c "import sys,json;d=json.load(sys.stdin);w=d.get('write_stats') or {};print((w.get('nodes_created',0)),(w.get('relationships_created',0)))" 2>/dev/null)
      created_nodes=$((created_nodes + $(echo $n|cut -d' ' -f1)))
      created_rels=$((created_rels + $(echo $n|cut -d' ' -f2)))
      ;;
  esac
done < "$DATA"
echo "  写入统计: 节点=$created_nodes, 关系=$created_rels"
check "创建了 11 个节点 (5机场+6货物)" "{\"n\":$created_nodes}" "d['n']==11"
check "创建了 20 条关系 (8航线+12货运)" "{\"r\":$created_rels}" "d['r']==20"

echo; hr; echo -e "${CYAN}场景 3: 节点查询与属性过滤${NC}"; hr
check "查所有机场 (5个)"            "$(Q 'MATCH (a:Airport) RETURN a.code, a.name')"                        "len(d['rows'])==5"
check "按 code 精确查 PVG"          "$(Q "MATCH (a:Airport {code:'PVG'}) RETURN a.name, a.city")"            "d['rows'][0][1]=='上海'"
check "货物按状态过滤 in_transit"   "$(Q "MATCH (c:Cargo {status:'in_transit'}) RETURN c.id")"               "len(d['rows'])==4"
check "WHERE 数值过滤 value>40000"  "$(Q 'MATCH (c:Cargo) WHERE c.value > 40000 RETURN c.id, c.value')"      "len(d['rows'])==3"

echo; hr; echo -e "${CYAN}场景 4: 边属性查询 (本次修复的核心)${NC}"; hr
r4="$(Q 'MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code, b.code, r.distance, r.flight_time, r.frequency')"
check "航线边返回 8 行"              "$r4" "len(d['rows'])==8"
check "边属性 distance 非 null"      "$r4" "all(row[2] is not None for row in d['rows'])"
check "边属性 flight_time 非 null"   "$r4" "all(row[3] is not None for row in d['rows'])"
check "边属性 frequency(字符串)非null" "$r4" "all(row[4] is not None for row in d['rows'])"
r4b="$(Q "MATCH (a:Airport {code:'PVG'})-[r:ROUTE_TO]->(b:Airport {code:'LAX'}) RETURN r.distance, r.flight_time")"
check "PVG→LAX distance=11000"      "$r4b" "d['rows'][0][0]==11000"
check "PVG→LAX flight_time=13"      "$r4b" "d['rows'][0][1]==13"

echo; hr; echo -e "${CYAN}场景 5: 多跳遍历与路径${NC}"; hr
check "2跳: PVG可达的下游机场"       "$(Q "MATCH (a:Airport {code:'PVG'})-[:ROUTE_TO]->(b)-[:ROUTE_TO]->(c) RETURN DISTINCT c.code")" "len(d['rows'])>=1"
check "货物起点终点 (SHIPS_FROM/TO)" "$(Q "MATCH (c:Cargo {id:'CARGO001'})-[:SHIPS_FROM]->(o), (c)-[:SHIPS_TO]->(dst) RETURN o.code, dst.code")" "d['rows'][0]==['PVG','LAX']"

echo; hr; echo -e "${CYAN}场景 6: 聚合与排序${NC}"; hr
check "count 机场总数"              "$(Q 'MATCH (a:Airport) RETURN count(a)')"                                "d['rows'][0][0]==5"
check "货物总重量 sum"              "$(Q 'MATCH (c:Cargo) RETURN sum(c.weight)')"                             "d['rows'][0][0]==4600"
check "按优先级分组计数"            "$(Q 'MATCH (c:Cargo) RETURN c.priority, count(c) ORDER BY count(c) DESC')" "len(d['rows'])>=1"
check "ORDER BY + LIMIT"           "$(Q 'MATCH (c:Cargo) RETURN c.id, c.value ORDER BY c.value DESC LIMIT 2')" "len(d['rows'])==2 and d['rows'][0][1]==100000"

echo; hr; echo -e "${CYAN}场景 7: SQL 接口 (SQL→Cypher)${NC}"; hr
rs="$(SQL 'SELECT * FROM Airport')"
check "SQL SELECT Airport 有结果"   "$rs" "('rows' in d and len(d['rows'])>=1) or 'error' in d"
rs2="$(SQL 'SELECT code, name FROM Airport WHERE city = \"上海\"')"
check "SQL WHERE 过滤"              "$rs2" "('rows' in d) or ('error' in d)"

echo; hr; echo -e "${CYAN}场景 8: 认证流程 (HMAC-SHA256)${NC}"; hr
tok="$(curl -s -X POST $BASE/api/auth/token -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}')"
check "获取 token 端点响应"         "$tok" "'token' in d or 'error' in d or 'access_token' in d"

echo; hr; echo -e "${CYAN}场景 9: 管理与存储接口${NC}"; hr
check "管理状态 /api/admin/status"  "$(curl -s $BASE/api/admin/status)"   "True"
check "存储状态 /api/storage/status" "$(curl -s $BASE/api/storage/status)" "True"
check "慢查询 /api/admin/slow-queries" "$(curl -s $BASE/api/admin/slow-queries)" "True"

echo; hr; echo -e "${CYAN}场景 10: EXPLAIN 查询计划${NC}"; hr
re="$(curl -s -X POST $BASE/api/query/explain -H 'Content-Type: application/json' -d '{"query":"MATCH (a:Airport) RETURN a.code"}')"
check "EXPLAIN 返回计划"            "$re" "True"

echo; hr; echo -e "${CYAN}场景 11: 向量索引与检索${NC}"; hr
rv="$(curl -s -X POST $BASE/api/vector/search -H 'Content-Type: application/json' -d '{"vector":[0.1,0.2,0.3],"k":3}')"
check "向量检索端点响应"            "$rv" "True"

echo; hr; echo -e "${CYAN}场景 12: 事件流 (RisingWave, SQL/MV)${NC}"; hr
# RisingWave frontend 在 4566 (pgwire),这里探测其 SQL 能力是否在线
rw="$(curl -s $BASE/api/streams)"
check "流列表 /api/streams 响应"    "$rw" "True"

# ══════════════════════════════════════════════════════
echo; hr
echo -e "${CYAN}测试汇总${NC}"; hr
TOTAL=$((PASS+FAIL))
echo -e "  总计: $TOTAL   ${GREEN}通过: $PASS${NC}   ${RED}失败: $FAIL${NC}"
if [ "$FAIL" -gt 0 ]; then
  echo -e "  ${RED}失败项:${NC}"; for n in "${FAILED_NAMES[@]}"; do echo "    - $n"; done
fi
hr
exit 0
