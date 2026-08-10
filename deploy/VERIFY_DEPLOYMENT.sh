#!/bin/bash
# 部署包验证脚本

set -e

echo "=========================================="
echo "Nexora 2.0 部署包验证"
echo "=========================================="
echo ""

PACKAGE="nexora-2.0-demo-macos-20260803.tar.gz"
TEST_DIR="/tmp/nexora-deploy-test-$$"

# 检查包是否存在
if [ ! -f "$PACKAGE" ]; then
    echo "❌ 错误: 找不到部署包 $PACKAGE"
    exit 1
fi

echo "1️⃣  创建测试目录..."
mkdir -p "$TEST_DIR"
echo "   ✅ 测试目录: $TEST_DIR"
echo ""

echo "2️⃣  解压部署包..."
tar -xzf "$PACKAGE" -C "$TEST_DIR"
echo "   ✅ 解压完成"
echo ""

echo "3️⃣  验证文件完整性..."
REQUIRED_FILES=(
    "nexora"
    "nexora.toml"
    "demo-data.cypher"
    "start-demo-simple.sh"
    "stop-demo-simple.sh"
    "README.md"
    "DEMO_GUIDE.md"
    "TESTING_CHECKLIST.md"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$TEST_DIR/$file" ]; then
        echo "   ✅ $file"
    else
        echo "   ❌ 缺失: $file"
        exit 1
    fi
done
echo ""

echo "4️⃣  验证二进制文件..."
if [ -x "$TEST_DIR/nexora" ]; then
    SIZE=$(ls -lh "$TEST_DIR/nexora" | awk '{print $5}')
    echo "   ✅ nexora 可执行 (大小: $SIZE)"
else
    echo "   ❌ nexora 不可执行"
    exit 1
fi
echo ""

echo "5️⃣  验证启动脚本..."
if [ -x "$TEST_DIR/start-demo-simple.sh" ]; then
    echo "   ✅ start-demo-simple.sh 可执行"
else
    echo "   ❌ start-demo-simple.sh 不可执行"
    exit 1
fi

if [ -x "$TEST_DIR/stop-demo-simple.sh" ]; then
    echo "   ✅ stop-demo-simple.sh 可执行"
else
    echo "   ❌ stop-demo-simple.sh 不可执行"
    exit 1
fi
echo ""

echo "6️⃣  验证文档完整性..."
README_SIZE=$(wc -c < "$TEST_DIR/README.md")
DEMO_SIZE=$(wc -c < "$TEST_DIR/DEMO_GUIDE.md")
TEST_SIZE=$(wc -c < "$TEST_DIR/TESTING_CHECKLIST.md")

if [ $README_SIZE -gt 1000 ]; then
    echo "   ✅ README.md ($README_SIZE bytes)"
else
    echo "   ❌ README.md 过小"
    exit 1
fi

if [ $DEMO_SIZE -gt 5000 ]; then
    echo "   ✅ DEMO_GUIDE.md ($DEMO_SIZE bytes)"
else
    echo "   ❌ DEMO_GUIDE.md 过小"
    exit 1
fi

if [ $TEST_SIZE -gt 3000 ]; then
    echo "   ✅ TESTING_CHECKLIST.md ($TEST_SIZE bytes)"
else
    echo "   ❌ TESTING_CHECKLIST.md 过小"
    exit 1
fi
echo ""

echo "7️⃣  清理测试目录..."
rm -rf "$TEST_DIR"
echo "   ✅ 清理完成"
echo ""

echo "=========================================="
echo "✅ 部署包验证通过！"
echo "=========================================="
echo ""
echo "部署包信息:"
PACKAGE_SIZE=$(ls -lh "$PACKAGE" | awk '{print $5}')
echo "  - 文件名: $PACKAGE"
echo "  - 大小: $PACKAGE_SIZE"
echo "  - 包含文件数: ${#REQUIRED_FILES[@]}"
echo ""
echo "可以安全交付给用户！"
echo ""
