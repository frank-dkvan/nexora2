# Nexora v0.6.0 发布清单

## 发布日期: 2026-07-10
## 状态: 准备中

## 前置条件

### 代码质量
- [x] P0 安全问题已全部修复
  - [x] P0-1: Quorum写入失败回滚机制
  - [x] P0-2: 复制日志持久化错误处理
  - [x] P0-3: WHERE子句短路求值
  - [x] P0-4: SQL注入防护
  - [x] P0-5: 资源耗尽攻击防护
  - [x] P0-6: 默认认证启用
  - [x] P0-7: 默认密钥安全
- [ ] 所有测试通过
  - [ ] 单元测试 (456个)
  - [ ] 集成测试 (35个)
  - [ ] 混沌测试
- [ ] 代码格式检查通过
- [ ] Clippy无警告

### 项目结构
- [x] 项目目录结构标准化
- [x] 配置文件模板创建
- [x] 构建脚本创建
- [x] 文档结构整理
- [x] .gitignore更新

### 文档
- [x] README.md 更新
- [x] PROJECT_STRUCTURE.md 创建
- [x] 配置示例文件创建
- [ ] CHANGELOG.md 更新 v0.6.0条目
- [ ] API文档验证
- [ ] 用户指南完善

### 构建和测试
- [ ] 发布版本构建成功
  ```bash
  scripts/build/build-release.sh
  ```
- [ ] Docker镜像构建成功
  ```bash
  scripts/build/build-docker.sh v0.6.0
  ```
- [ ] 所有脚本测试通过
- [ ] 配置模板验证

### 安全检查
- [ ] 依赖安全审计
  ```bash
  cargo audit
  ```
- [ ] 默认配置安全验证
- [ ] 安全文档审查

## 发布步骤

### 1. 版本号更新
- [ ] 更新 `Cargo.toml` workspace.package.version 为 "0.6.0"
- [ ] 更新所有使用硬编码版本的文档
- [ ] 生成新的 Cargo.lock

### 2. 更新 CHANGELOG.md
```markdown
## [0.6.0] - 2026-07-10

### Added
- 标准化配置文件模板 (dev/prod/cluster)
- Docker Compose部署配置
- 构建和测试脚本
- 完整的项目结构文档

### Fixed
- P0-1: Quorum写入失败回滚机制
- P0-2: 复制日志持久化错误处理
- P0-3: WHERE子句短路求值
- P0-4: SQL注入防护
- P0-5: 资源耗尽攻击防护
- P0-6: 默认认证配置
- P0-7: 默认密钥安全

### Changed
- 项目目录结构标准化
- 文档重组和改进
- 可执行文件名称确认 (nexora/nex/nexora-mcp)

### Security
- 默认启用认证
- 强制生产环境TLS
- 增强输入验证
- 改进错误消息清理
```

### 3. 构建发布制品
```bash
# 构建所有平台
scripts/build/build-release.sh
scripts/build/build-release.sh --target x86_64-unknown-linux-gnu
scripts/build/build-release.sh --target aarch64-unknown-linux-gnu
scripts/build/build-release.sh --target x86_64-apple-darwin
scripts/build/build-release.sh --target aarch64-apple-darwin

# 构建Docker镜像
scripts/build/build-docker.sh v0.6.0

# 打包
cd target/release
tar -czf nexora-v0.6.0-linux-x86_64.tar.gz nexora nex nexora-mcp
tar -czf nexora-v0.6.0-macos-arm64.tar.gz nexora nex nexora-mcp
```

### 4. 测试发布制品
- [ ] 解压并运行各平台二进制文件
- [ ] 验证版本号输出正确
- [ ] 测试基本功能
- [ ] Docker镜像运行测试

### 5. 创建Git标签
```bash
git add .
git commit -m "Release v0.6.0 - Production readiness improvements"
git tag -a v0.6.0 -m "Release v0.6.0

Major improvements:
- Fixed all P0 security issues
- Standardized project structure
- Added configuration templates
- Improved documentation
"
git push origin main
git push origin v0.6.0
```

### 6. 发布到GitHub Release
- [ ] 创建GitHub Release v0.6.0
- [ ] 上传发布制品
  - nexora-v0.6.0-linux-x86_64.tar.gz
  - nexora-v0.6.0-macos-arm64.tar.gz
  - nexora-v0.6.0-windows-x86_64.zip (如果支持)
- [ ] 复制CHANGELOG内容到Release描述
- [ ] 标记为Pre-release或正式Release

### 7. Docker镜像发布
```bash
# 标记镜像
docker tag nexora:v0.6.0 your-registry/nexora:0.6.0
docker tag nexora:v0.6.0 your-registry/nexora:latest

# 推送到镜像仓库
docker push your-registry/nexora:0.6.0
docker push your-registry/nexora:latest
```

### 8. 文档更新
- [ ] 更新在线文档（如果有）
- [ ] 更新README徽章
- [ ] 社交媒体公告（如果适用）

## 发布后验证

### 功能验证
- [ ] 使用发布的二进制文件启动服务
- [ ] 运行快速开始指南中的示例
- [ ] 验证配置文件模板工作正常
- [ ] 测试Docker Compose部署

### 性能验证
- [ ] 运行基准测试套件
- [ ] 验证性能无退化

### 文档验证
- [ ] 所有链接有效
- [ ] 示例代码可运行
- [ ] 配置说明准确

## 回滚计划

如果发现严重问题：

1. 删除GitHub Release
2. 删除Git标签
   ```bash
   git tag -d v0.6.0
   git push origin :refs/tags/v0.6.0
   ```
3. 从Docker镜像仓库删除标签
4. 发布公告说明情况

## 发布团队

- 发布经理: [待定]
- 测试负责人: [待定]
- 文档负责人: [待定]
- 基础设施负责人: [待定]

## 发布时间表

- [ ] T-7天: 代码冻结
- [ ] T-5天: 完成所有测试
- [ ] T-3天: 文档审查
- [ ] T-1天: 构建发布制品
- [ ] T-0: 发布
- [ ] T+1天: 发布后监控
- [ ] T+7天: 发布回顾

## 备注

- 这是从v0.1.0到v0.6.0的重大版本跳跃
- 主要原因: Phase 1 P0问题修复完成，项目结构标准化
- 建议作为Beta版本发布，收集社区反馈
- 考虑在发布前进行社区测试期

## 相关文档

- [项目结构整理总结](PROJECT_STRUCTURE_CLEANUP_SUMMARY.md)
- [综合代码审查](docs/COMPREHENSIVE_CODE_REVIEW_2026-07-10.md)
- [生产部署计划](docs/production-planning/PRODUCTION_DEPLOYMENT_PLAN.md)
- [生产就绪差距](docs/production-planning/PRODUCTION_READINESS_GAPS.md)
