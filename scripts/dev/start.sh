#!/bin/bash
# Nexora-RS 完整启动脚本 (后端 + 前端 + 样例数据)

set -e

echo "🚀 启动 Nexora-RS 完整系统..."
echo ""

# 检查依赖
command -v cargo >/dev/null 2>&1 || { echo "❌ 需要安装 Rust/Cargo"; exit 1; }
command -v node >/dev/null 2>&1 || { echo "❌ 需要安装 Node.js"; exit 1; }

# 编译后端
echo "📦 编译 Rust 后端..."
cargo build --release

# 安装前端依赖
echo "📦 安装前端依赖..."
cd ui
npm install
cd ..

# 启动后端 (后台运行)
echo "🔧 启动后端服务 (端口 8080)..."
./target/release/nexora-app --host 0.0.0.0 --port 8080 --num-shards 16 &
BACKEND_PID=$!
echo "后端 PID: $BACKEND_PID"

# 等待后端就绪
echo "⏳ 等待后端就绪..."
for i in {1..30}; do
  if curl -s http://localhost:8080/api/v2/health > /dev/null 2>&1; then
    echo "✅ 后端已就绪"
    break
  fi
  sleep 1
  if [ $i -eq 30 ]; then
    echo "❌ 后端启动超时"
    kill $BACKEND_PID 2>/dev/null || true
    exit 1
  fi
done

# 导入样例数据
if [ -f sample-data.jsonl ]; then
  echo "📊 导入样例数据..."
  SAMPLE_PATH=$(pwd)/sample-data.jsonl
  curl -X POST http://localhost:8080/api/v2/ingest/file \
    -H "Content-Type: application/json" \
    -d "{\"path\":\"$SAMPLE_PATH\",\"id_field\":\"id\"}" \
    -s | jq . || echo "数据导入已触发"
fi

# 启动前端 (后台运行)
echo "🎨 启动前端开发服务器 (端口 3000)..."
cd ui
npm run dev &
FRONTEND_PID=$!
cd ..
echo "前端 PID: $FRONTEND_PID"

echo ""
echo "✅ 启动完成!"
echo ""
echo "📍 访问地址:"
echo "   前端: http://localhost:3000"
echo "   后端: http://localhost:8080"
echo ""
echo "🛠️  停止服务:"
echo "   kill $BACKEND_PID $FRONTEND_PID"
echo "   或者运行: ./STOP.sh"
echo ""
echo "💾 样例数据已导入 (10 条记录)"
echo ""

# 保存 PID
echo "$BACKEND_PID" > .backend.pid
echo "$FRONTEND_PID" > .frontend.pid

# 保持脚本运行
wait
