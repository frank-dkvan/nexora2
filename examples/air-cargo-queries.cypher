// ========================================
// 航空货运站图数据库 - 演示查询
// ========================================
// 使用方法：
// 1. 先执行 air-cargo-demo.cypher 创建数据
// 2. 再执行本文件中的查询进行测试

// ========================================
// 查询1：查找所有机场及其货运站信息
// ========================================
MATCH (a:Airport)-[r:HAS_TERMINAL]->(t:CargoTerminal)
RETURN a.code AS 机场代码,
       a.name AS 机场名称,
       a.city AS 城市,
       t.terminal_id AS 货运站ID,
       t.area_sqm AS 面积平方米,
       t.warehouse_capacity_tons AS 仓储容量吨,
       t.cold_storage AS 冷链仓储,
       t.dangerous_goods_certified AS 危险品认证
ORDER BY a.capacity_tons_per_day DESC;

// ========================================
// 查询2：查找从上海浦东到洛杉矶的所有航线
// ========================================
MATCH (origin:Airport {code: "PVG"})-[r:ROUTE]->(dest:Airport {code: "LAX"})
RETURN r.airline AS 航空公司,
       r.flight_number AS 航班号,
       r.frequency_per_week AS 每周班次,
       r.flight_time_hours AS 飞行时间小时,
       r.aircraft_type AS 机型,
       r.max_cargo_tons AS 最大载货吨;

// ========================================
// 查询3：查找所有在途货物及其详细信息
// ========================================
MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE s.status = "在途"
RETURN s.awb_number AS 运单号,
       s.shipper_name AS 发货人,
       s.consignee_name AS 收货人,
       s.origin AS 起运地,
       s.destination AS 目的地,
       ct.category AS 货物类型,
       s.weight_kg AS 重量千克,
       s.declared_value_usd AS 申报价值美元,
       s.scheduled_arrival AS 预计到达时间;

// ========================================
// 查询4：查找需要冷链运输的所有货物
// ========================================
MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE ct.requires_temperature_control = true
RETURN s.awb_number AS 运单号,
       ct.category AS 货物类型,
       ct.temp_range_celsius AS 温度要求摄氏度,
       s.origin AS 起运地,
       s.destination AS 目的地,
       s.status AS 状态,
       s.weight_kg AS 重量千克;

// ========================================
// 查询5：查找某货运站当前存储的所有货物
// ========================================
MATCH (s:Shipment)-[r:STORED_AT]->(t:CargoTerminal {terminal_id: "PVG-T1"})
RETURN s.awb_number AS 运单号,
       s.shipper_name AS 发货人,
       s.weight_kg AS 重量千克,
       r.warehouse_location AS 仓位,
       r.check_in_time AS 入库时间,
       r.handler AS 操作员,
       r.temperature_celsius AS 当前温度,
       s.status AS 状态
ORDER BY r.check_in_time DESC;

// ========================================
// 查询6：计算从北京到纽约的最优货运路径
// ========================================
// 直飞路径
MATCH path = (origin:Airport {code: "PEK"})-[r:ROUTE]->(dest:Airport {code: "JFK"})
RETURN "直飞" AS 路径类型,
       [n in nodes(path) | n.code] AS 途经机场,
       r.airline AS 航空公司,
       r.flight_number AS 航班号,
       r.flight_time_hours AS 总飞行时间小时,
       r.max_cargo_tons AS 最大载货吨;

// 经停路径（最多2段）
MATCH path = (origin:Airport {code: "PEK"})-[:ROUTE*1..2]->(dest:Airport {code: "JFK"})
WITH path,
     [r in relationships(path) | r.flight_time_hours] AS segments,
     [n in nodes(path) | n.code] AS airports
RETURN "经停" AS 路径类型,
       airports AS 途经机场,
       reduce(total = 0, time in segments | total + time) AS 总飞行时间小时,
       length(path) AS 航段数
ORDER BY 总飞行时间小时 ASC
LIMIT 3;

// ========================================
// 查询7：统计各机场的货物吞吐量
// ========================================
MATCH (s:Shipment)
WITH s.origin AS airport_code, sum(s.weight_kg) AS total_outbound_kg
RETURN airport_code AS 机场代码,
       total_outbound_kg AS 出港总重量千克,
       total_outbound_kg / 1000.0 AS 出港总重量吨
ORDER BY total_outbound_kg DESC;

// ========================================
// 查询8：查找高价值货物（>10万美元）
// ========================================
MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE s.declared_value_usd > 100000
RETURN s.awb_number AS 运单号,
       s.shipper_name AS 发货人,
       ct.category AS 货物类型,
       s.weight_kg AS 重量千克,
       s.declared_value_usd AS 申报价值美元,
       s.declared_value_usd / s.weight_kg AS 单位价值美元每千克,
       s.status AS 状态
ORDER BY s.declared_value_usd DESC;

// ========================================
// 查询9：查找某航空公司运营的所有航线
// ========================================
MATCH (origin:Airport)-[r:ROUTE {airline: "CA"}]->(dest:Airport)
RETURN origin.code AS 起点,
       origin.name AS 起点名称,
       dest.code AS 终点,
       dest.name AS 终点名称,
       r.flight_number AS 航班号,
       r.frequency_per_week AS 每周班次,
       r.aircraft_type AS 机型
ORDER BY origin.code, dest.code;

// ========================================
// 查询10：查找具备危险品处理资质的货运站
// ========================================
MATCH (a:Airport)-[:HAS_TERMINAL]->(t:CargoTerminal)
WHERE t.dangerous_goods_certified = true
RETURN a.code AS 机场代码,
       a.name AS 机场名称,
       a.country AS 国家,
       t.terminal_id AS 货运站ID,
       t.area_sqm AS 面积平方米,
       t.warehouse_capacity_tons AS 仓储容量吨
ORDER BY t.warehouse_capacity_tons DESC;

// ========================================
// 查询11：分析货物类型分布
// ========================================
MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
RETURN ct.category AS 货物类型,
       count(s) AS 运单数量,
       sum(s.weight_kg) AS 总重量千克,
       sum(s.declared_value_usd) AS 总价值美元,
       avg(s.declared_value_usd / s.weight_kg) AS 平均单价美元每千克
ORDER BY 总价值美元 DESC;

// ========================================
// 查询12：查找待处理的货物（按优先级）
// ========================================
MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE s.status IN ["待装机", "仓储中"]
WITH s, ct,
     CASE
       WHEN ct.requires_temperature_control = true THEN 1
       WHEN s.declared_value_usd > 100000 THEN 2
       ELSE 3
     END AS priority
RETURN s.awb_number AS 运单号,
       s.origin AS 起运地,
       s.destination AS 目的地,
       ct.category AS 货物类型,
       s.status AS 状态,
       s.scheduled_departure AS 计划起飞时间,
       priority AS 优先级,
       CASE priority
         WHEN 1 THEN "高优先级-冷链"
         WHEN 2 THEN "中优先级-高价值"
         ELSE "普通优先级"
       END AS 优先级说明
ORDER BY priority ASC, s.scheduled_departure ASC;

// ========================================
// 查询13：查找枢纽机场（连接数最多）
// ========================================
MATCH (a:Airport)-[r:ROUTE]->(:Airport)
WITH a, count(r) AS outbound_routes
MATCH (a)<-[r2:ROUTE]-(:Airport)
WITH a, outbound_routes, count(r2) AS inbound_routes
RETURN a.code AS 机场代码,
       a.name AS 机场名称,
       a.country AS 国家,
       outbound_routes AS 出港航线数,
       inbound_routes AS 进港航线数,
       outbound_routes + inbound_routes AS 总航线数
ORDER BY 总航线数 DESC;

// ========================================
// 查询14：查找需要中转的货物及推荐路径
// ========================================
MATCH (s:Shipment)
WHERE s.status IN ["待装机", "仓储中"]
WITH s
MATCH (origin:Airport {code: s.origin}), (dest:Airport {code: s.destination})
OPTIONAL MATCH direct = (origin)-[r:ROUTE]->(dest)
WITH s, origin, dest, direct, r
WHERE direct IS NULL  // 只显示没有直飞的
MATCH path = (origin)-[:ROUTE*1..2]->(dest)
WITH s, path,
     [rel in relationships(path) | rel.flight_time_hours] AS times,
     [n in nodes(path) | n.code] AS airports,
     [rel in relationships(path) | rel.airline] AS airlines
RETURN s.awb_number AS 运单号,
       airports[0] AS 起点,
       airports[-1] AS 终点,
       airports AS 途经机场,
       airlines AS 承运航司,
       reduce(total = 0, t in times | total + t) AS 总飞行时间小时,
       length(path) AS 中转次数
ORDER BY s.awb_number, 总飞行时间小时 ASC
LIMIT 10;

// ========================================
// 查询15：计算货运站使用率
// ========================================
MATCH (t:CargoTerminal)
OPTIONAL MATCH (s:Shipment)-[:STORED_AT]->(t)
WITH t, sum(s.weight_kg) AS current_weight_kg
RETURN t.terminal_id AS 货运站ID,
       t.airport_code AS 机场代码,
       t.warehouse_capacity_tons AS 容量吨,
       current_weight_kg / 1000.0 AS 当前存储吨,
       round((current_weight_kg / (t.warehouse_capacity_tons * 1000.0)) * 100, 2) AS 使用率百分比,
       CASE
         WHEN current_weight_kg / (t.warehouse_capacity_tons * 1000.0) > 0.8 THEN "告警-接近满载"
         WHEN current_weight_kg / (t.warehouse_capacity_tons * 1000.0) > 0.5 THEN "正常-过半"
         ELSE "空闲"
       END AS 状态评估
ORDER BY 使用率百分比 DESC;

// ========================================
// 查询说明：
// - 所有查询都使用真实的航空货运业务场景
// - 可以通过修改参数（机场代码、状态等）进行自定义查询
// - 支持复杂的图遍历和路径分析
// - 演示了 Nexora 的 Cypher 查询能力
// ========================================
