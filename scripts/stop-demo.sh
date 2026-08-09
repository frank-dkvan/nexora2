#!/bin/bash
# ========================================
# Nexora 演示 - 停止服务器脚本
# ========================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data/demo"
PID_FILE="$DATA_DIR/server.pid"

echo "========================================="
echo "🛑 停止 Nexora 服务器"
echo "========================================="
echo ""

if [ ! -f "$PID_FILE" ]; then
    echo "⚠️  未找到 PID 文件: $PID_FILE"
    echo "   尝试查找运行中的 nexora-app 进程..."

    PIDS=$(pgrep -f "nexora-app.*--config")

    if [ -z "$PIDS" ]; then
        echo "❌ 未找到运行中的 Nexora 服务器"
        exit 1
    fi

    echo "   找到以下进程:"
    ps -p $PIDS -o pid,command
    echo ""

    read -p "是否停止这些进程? (y/N): " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Yy]$ ]]; then
        for pid in $PIDS; do
            echo "   停止进程 $pid..."
            kill $pid
            sleep 1

            if kill -0 $pid 2>/dev/null; then
                echo "   强制停止进程 $pid..."
                kill -9 $pid
            fi
        done
        echo "✅ 服务器已停止"
    else
        echo "❌ 取消操作"
        exit 0
    fi
else
    SERVER_PID=$(cat "$PID_FILE")

    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "⚠️  进程 $SERVER_PID 不存在（可能已经停止）"
        rm -f "$PID_FILE"
        exit 0
    fi

    echo "停止服务器进程: $SERVER_PID"

    # 优雅停止
    kill $SERVER_PID

    # 等待进程结束
    for i in {1..10}; do
        if ! kill -0 $SERVER_PID 2>/dev/null; then
            echo "✅ 服务器已停止"
            rm -f "$PID_FILE"
            exit 0
        fi
        sleep 1
        echo -n "."
    done

    echo ""
    echo "⚠️  服务器未响应，强制停止..."
    kill -9 $SERVER_PID

    sleep 1

    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "✅ 服务器已强制停止"
        rm -f "$PID_FILE"
    else
        echo "❌ 无法停止服务器进程"
        exit 1
    fi
fi

echo ""
echo "💡 提示："
echo "   - 数据已保留在: $DATA_DIR"
echo "   - 重新启动: ./scripts/quick-start.sh"
echo ""
