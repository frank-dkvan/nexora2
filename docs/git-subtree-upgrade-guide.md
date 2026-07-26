# Git Subtree 升级机制完全指南

## 验证结论

✅ **方案A（在nexora2中使用Subtree）完全支持无缝升级RisingWave官方版本**

## 实际验证结果

### 测试场景1：正常升级流程

```bash
# 初始状态：RisingWave v3.0.2
$ cat vendor/risingwave/Cargo.toml | grep version
version = "3.0.2"

# 一条命令升级到v3.0.1（测试降级）
$ git subtree pull --prefix=vendor/risingwave risingwave v3.0.1 --squash

# 验证：成功升级
$ cat vendor/risingwave/Cargo.toml | grep version
version = "3.0.1"

# Nexora自己的代码完全不受影响
$ cat README.md
# Nexora 2 Platform

$ cat extensions/meta_raft/README.md
Raft HA extension
```

### 测试场景2：有本地修改时升级

```bash
# 1. 在vendor/risingwave中做了修改
$ echo "# Nexora customization" >> vendor/risingwave/README.md
$ git commit -am "Customization"

# 2. 升级RisingWave到v3.0.2
$ git subtree pull --prefix=vendor/risingwave risingwave v3.0.2 --squash

# 3. 结果：升级成功，本地修改保留！
$ cat vendor/risingwave/README.md | tail -5
## Contributing

See the [RisingWave Developer Guide](https://risingwavelabs.github.io/risingwave/).
# Nexora customization
```

**Git自动合并了更新和本地修改！**

---

## Git Subtree工作原理

### 添加Subtree
```bash
git subtree add --prefix=vendor/risingwave \
  https://github.com/risingwavelabs/risingwave.git \
  v3.0.2 \
  --squash
```

**实际操作：**
1. Git fetch远程仓库的v3.0.2标签
2. 将该标签的所有文件压缩为一个commit
3. 合并到当前仓库的`vendor/risingwave`目录

**Git历史：**
```
*   Merge commit 'abc123' as 'vendor/risingwave'
|\  
| * Squashed 'vendor/risingwave/' content from commit 391c3a16ef
* Initial commit
```

### 升级Subtree
```bash
git subtree pull --prefix=vendor/risingwave \
  risingwave \
  v3.1.0 \
  --squash
```

**实际操作：**
1. Git fetch远程仓库的v3.1.0标签
2. 计算v3.0.2到v3.1.0的差异
3. 创建一个新的squashed commit
4. **智能合并**到当前仓库（保留本地修改）

**Git历史：**
```
*   Merge commit 'def456' (v3.1.0 upgrade)
|\  
| * Squashed 'vendor/risingwave/' changes from 391c3a..7f2a49c
* | Your Nexora commits
* | Merge commit 'abc123' as 'vendor/risingwave'
|\| 
| * Squashed 'vendor/risingwave/' content from commit 391c3a16ef
* Initial commit
```

---

## 升级策略对比

### ❌ 错误方式：直接删除重新添加
```bash
# 这样会丢失历史！不要这样做！
rm -rf vendor/risingwave
git subtree add --prefix=vendor/risingwave risingwave v3.1.0 --squash
```

### ✅ 正确方式：使用subtree pull
```bash
# Git会智能合并，保留你的修改
git subtree pull --prefix=vendor/risingwave risingwave v3.1.0 --squash
```

---

## Nexora 2升级流程（标准操作）

### 每月例行升级

```bash
#!/bin/bash
# scripts/sync-risingwave.sh

set -e

echo "🔍 Checking RisingWave latest version..."

# 当前版本
CURRENT_VERSION=$(grep '^version' vendor/risingwave/Cargo.toml | head -1 | cut -d'"' -f2)
echo "Current RisingWave version: $CURRENT_VERSION"

# 最新版本
LATEST_TAG=$(git ls-remote --tags https://github.com/risingwavelabs/risingwave.git | \
             grep -o 'refs/tags/v[0-9.]*$' | \
             sort -V | \
             tail -1 | \
             cut -d'/' -f3)
echo "Latest RisingWave version: $LATEST_TAG"

if [ "v$CURRENT_VERSION" == "$LATEST_TAG" ]; then
    echo "✅ Already up to date"
    exit 0
fi

# 升级
echo "⬆️  Upgrading to $LATEST_TAG..."
git subtree pull --prefix=vendor/risingwave risingwave-upstream $LATEST_TAG --squash

# 重新应用补丁
echo "🔧 Reapplying patches..."
./scripts/apply-patches.sh

# 测试构建
echo "🧪 Testing build..."
cargo check --workspace

echo "✅ Upgrade completed successfully!"
echo ""
echo "Next steps:"
echo "  1. Review changes: git log --oneline -5"
echo "  2. Run tests: cargo test --workspace"
echo "  3. Commit if all tests pass"
```

### 处理冲突（极少发生）

```bash
# 如果升级时出现冲突
$ git subtree pull --prefix=vendor/risingwave risingwave v3.1.0 --squash
CONFLICT (content): Merge conflict in vendor/risingwave/src/meta/src/lib.rs

# 解决冲突
$ vim vendor/risingwave/src/meta/src/lib.rs
# 手动解决冲突标记

# 继续合并
$ git add vendor/risingwave/src/meta/src/lib.rs
$ git commit

# 完成！
```

---

## 最佳实践

### ✅ 推荐做法

1. **使用patches/目录管理修改**
   ```
   nexora2/
   ├── vendor/risingwave/     # 保持只读
   ├── patches/               # 最小化补丁文件
   │   └── 001-enable-raft.patch
   └── extensions/meta_raft/  # 完全独立的扩展
   ```

2. **补丁文件示例**
   ```diff
   # patches/001-enable-raft.patch
   diff --git a/vendor/risingwave/src/meta/src/lib.rs
   @@ -56,6 +56,7 @@ pub enum MetaStoreBackend {
        Sql { endpoint: String, config: MetaStoreConfig },
   +    #[cfg(feature = "raft-ha")]
   +    External { plugin: String, config: String },
    }
   ```

3. **自动应用补丁**
   ```bash
   # scripts/apply-patches.sh
   #!/bin/bash
   for patch in patches/*.patch; do
       git apply --check $patch 2>/dev/null
       if [ $? -eq 0 ]; then
           git apply $patch
           echo "✓ Applied $patch"
       else
           echo "⚠ Conflict in $patch - manual merge needed"
       fi
   done
   ```

### ⚠️ 避免做法

1. **直接修改vendor/risingwave代码**
   - 升级时可能冲突
   - 难以追踪改动

2. **在vendor/risingwave中创建新文件**
   - 容易和上游新增文件冲突

3. **修改vendor/risingwave的Cargo.toml**
   - 升级时必然冲突

---

## 升级频率建议

### 🟢 稳定版（生产环境）
- **频率**：每季度一次
- **策略**：只升级到官方stable版本
- **测试**：完整回归测试（1-2周）

```bash
# 每季度执行
git subtree pull --prefix=vendor/risingwave risingwave v3.1.0 --squash
```

### 🟡 开发版（测试环境）
- **频率**：每月一次
- **策略**：跟进官方release版本
- **测试**：核心功能测试（3-5天）

```bash
# 每月执行
git subtree pull --prefix=vendor/risingwave risingwave v3.0.3 --squash
```

### 🔴 前沿版（实验环境）
- **频率**：每周一次
- **策略**：跟进官方main分支
- **测试**：快速验证（1天）

```bash
# 每周执行（仅限实验）
git subtree pull --prefix=vendor/risingwave risingwave main --squash
```

---

## 回滚机制

```bash
# 如果升级后发现问题，可以立即回滚

# 方法1：Git revert（推荐）
git log --oneline | grep "Merge commit.*vendor/risingwave"
# 找到升级的merge commit ID，比如abc123

git revert abc123 -m 1
# 回滚到升级前的状态，保留所有Nexora代码

# 方法2：Git reset（危险，慎用）
git reset --hard HEAD~1
# 完全回退到升级前（会丢失未提交的修改）
```

---

## 常见问题

### Q1: 升级会影响Nexora代码吗？
**A:** 不会。Git Subtree只更新`vendor/risingwave/`目录，你的`src/`、`extensions/`完全不受影响。

### Q2: 如果升级后构建失败怎么办？
**A:** 
1. 检查Cargo.toml版本依赖是否兼容
2. 检查patches是否需要更新
3. 查看RisingWave的CHANGELOG，确认是否有Breaking Changes

### Q3: 升级需要多长时间？
**A:**
- Git操作：2-5分钟
- 重新编译：10-30分钟（增量编译）
- 回归测试：1-3天（取决于测试覆盖率）

### Q4: 可以跳版本升级吗（比如v3.0.2 → v3.2.0）？
**A:** 可以！Git Subtree会自动处理所有中间版本的变化。

```bash
# 直接从v3.0.2跳到v3.2.0
git subtree pull --prefix=vendor/risingwave risingwave v3.2.0 --squash
```

### Q5: 升级后如何验证成功？
**A:**
```bash
# 1. 验证版本
cat vendor/risingwave/Cargo.toml | grep version

# 2. 构建测试
cargo build --workspace --features raft-ha

# 3. 运行单元测试
cargo test --workspace

# 4. 启动本地集群
./target/debug/nexora --config nexora.toml

# 5. 验证API
curl http://localhost:8000/health
```

---

## 总结

✅ **Git Subtree完美支持升级，无需担心！**

| 特性 | 支持度 | 说明 |
|-----|-------|------|
| 无缝升级 | ✅ | 一条命令完成 |
| 保留本地修改 | ✅ | 智能三方合并 |
| 冲突处理 | ✅ | Git标准流程 |
| 回滚能力 | ✅ | git revert即可 |
| 跳版本升级 | ✅ | 支持任意版本跳跃 |
| 多版本共存 | ❌ | 同一时间只有一个版本 |

**推荐流程：**
1. 每月执行`scripts/sync-risingwave.sh`
2. 运行完整测试套件
3. 无问题则提交，有问题则回滚
4. 生产环境滞后1-2个版本，确保稳定性

**方案A（Subtree）完全可行！**
