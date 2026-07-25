-- ============================================================
-- PG-Wire + Standing Query + Materialized View 演示
-- ============================================================
-- 这个演示展示了完整的数据管道：
-- SQL写入 → 图数据库 → Standing Query匹配 → 物化视图更新 → SQL查询
-- ============================================================

-- 1. 创建传感器数据（通过PG-Wire）
-- 这些INSERT会写入分布式图数据库
INSERT INTO Sensor (sensor_id, temperature, location) VALUES ('sensor_001', 85, 'Room A');
INSERT INTO Sensor (sensor_id, temperature, location) VALUES ('sensor_002', 70, 'Room B');
INSERT INTO Sensor (sensor_id, temperature, location) VALUES ('sensor_003', 95, 'Room C');
INSERT INTO Sensor (sensor_id, temperature, location) VALUES ('sensor_004', 65, 'Room D');

-- 2. Standing Query自动检测高温传感器（temperature > 80）
--    匹配结果自动更新到物化视图 'hot_sensors'

-- 3. 查询物化视图（通过PG-Wire）
-- 这个查询会直接访问物化视图，而不是扫描整个图
SELECT * FROM hot_sensors;
-- 预期结果：sensor_001 (85°C) 和 sensor_003 (95°C)

-- 4. 更新传感器温度
UPDATE Sensor SET temperature = 50 WHERE sensor_id = 'sensor_001';
-- Standing Query检测到不再匹配，自动从物化视图中删除

-- 5. 再次查询物化视图
SELECT * FROM hot_sensors;
-- 预期结果：只有 sensor_003 (95°C)

-- 6. 添加新的高温传感器
INSERT INTO Sensor (sensor_id, temperature, location) VALUES ('sensor_005', 92, 'Room E');
-- Standing Query检测到匹配，自动添加到物化视图

-- 7. 最终查询
SELECT * FROM hot_sensors ORDER BY temperature DESC;
-- 预期结果：sensor_003 (95°C) 和 sensor_005 (92°C)

-- ============================================================
-- 关键特性：
-- ============================================================
-- ✅ 标准SQL接口：使用任何PostgreSQL客户端
-- ✅ 实时模式匹配：Standing Query自动检测数据变化
-- ✅ 增量更新：物化视图只包含匹配的数据
-- ✅ 高性能查询：直接查询预计算的物化视图
-- ✅ 事件驱动：可以订阅匹配事件发送到下游系统
-- ============================================================
