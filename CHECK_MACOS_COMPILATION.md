# macOS ARM64 编译方案调查

## 目标
找到可以在 macOS ARM64 上成功编译的 RisingWave 版本或方法

## 调查方向

### 方向 1: 测试不同的 RisingWave 版本
- v3.0.2 ❌ 失败（已验证）
- v3.0.1 ？
- v3.0.0 ？
- v2.x.x ？
- main 分支 ？

### 方向 2: 使用 RisingWave 官方二进制
- 检查是否提供 macOS ARM64 预编译版本
- GitHub Releases 页面

### 方向 3: 交叉编译
- 在 macOS 上交叉编译到 Linux
- 使用 cross 工具

### 方向 4: 社区反馈
- 检查 RisingWave GitHub Issues
- 搜索 macOS compilation 相关问题

让我开始调查...
