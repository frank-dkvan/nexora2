#!/bin/bash
# 停止所有 Nexora-RS 服务

echo "🛑 停止 Nexora-RS 服务..."

# 从 PID 文件停止
if [ -f .backend.pid ]; then
  BACKEND_PID=$(cat .backend.pid)
  echo "停止后端 (PID: $BACKEND_PID)..."
  kill $BACKEND_PID 2>/dev/null && echo "✅ 后端已停止" || echo "⚠️  后端进程未运行"
  rm .backend.pid
fi

if [ -f .frontend.pid ]; then
  FRONTEND_PID=$(cat .frontend.pid)
  echo "停止前端 (PID: $FRONTEND_PID)..."
  kill $FRONTEND_PID 2>/dev/null && echo "✅ 前端已停止" || echo "⚠️  前端进程未运行"
  rm .frontend.pid
fi

# 备用清理：按端口停止
echo "清理残留进程..."
lsof -ti:8080 | xargs kill -9 2>/dev/null && echo "清理端口 8080" || true
lsof -ti:3000 | xargs kill -9 2>/dev/null && echo "清理端口 3000" || true

echo "✅ 所有服务已停止"
