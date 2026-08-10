// ========================================
// 航空货运站图数据库演示
// ========================================
// 场景：国际航空货运网络管理系统
// 涵盖：机场、货运站、航线、货物、运输商、仓储、清关

// ========================================
// 1. 创建机场节点
// ========================================

CREATE (pek:Airport {
  code: "PEK",
  name: "北京首都国际机场",
  city: "北京",
  country: "中国",
  timezone: "Asia/Shanghai",
  capacity_tons_per_day: 5000,
  operational_hours: 24
});

CREATE (pvg:Airport {
  code: "PVG",
  name: "上海浦东国际机场",
  city: "上海",
  country: "中国",
  timezone: "Asia/Shanghai",
  capacity_tons_per_day: 4500,
  operational_hours: 24
});

CREATE (can:Airport {
  code: "CAN",
  name: "广州白云国际机场",
  city: "广州",
  country: "中国",
  timezone: "Asia/Shanghai",
  capacity_tons_per_day: 3800,
  operational_hours: 24
});

CREATE (hkg:Airport {
  code: "HKG",
  name: "香港国际机场",
  city: "香港",
  country: "中国",
  timezone: "Asia/Hong_Kong",
  capacity_tons_per_day: 4200,
  operational_hours: 24
});

CREATE (sin:Airport {
  code: "SIN",
  name: "新加坡樟宜机场",
  city: "新加坡",
  country: "新加坡",
  timezone: "Asia/Singapore",
  capacity_tons_per_day: 3500,
  operational_hours: 24
});

CREATE (lax:Airport {
  code: "LAX",
  name: "洛杉矶国际机场",
  city: "洛杉矶",
  country: "美国",
  timezone: "America/Los_Angeles",
  capacity_tons_per_day: 6000,
  operational_hours: 24
});

CREATE (jfk:Airport {
  code: "JFK",
  name: "纽约肯尼迪国际机场",
  city: "纽约",
  country: "美国",
  timezone: "America/New_York",
  capacity_tons_per_day: 5500,
  operational_hours: 24
});

CREATE (fra:Airport {
  code: "FRA",
  name: "法兰克福机场",
  city: "法兰克福",
  country: "德国",
  timezone: "Europe/Berlin",
  capacity_tons_per_day: 4800,
  operational_hours: 24
});

// ========================================
// 2. 创建货运站节点
// ========================================

CREATE (pek_cargo:CargoTerminal {
  terminal_id: "PEK-T1",
  airport_code: "PEK",
  name: "首都机场货运站1号",
  area_sqm: 50000,
  cold_storage: true,
  dangerous_goods_certified: true,
  customs_clearance: true,
  warehouse_capacity_tons: 2000
});

CREATE (pvg_cargo:CargoTerminal {
  terminal_id: "PVG-T1",
  airport_code: "PVG",
  name: "浦东机场货运站1号",
  area_sqm: 60000,
  cold_storage: true,
  dangerous_goods_certified: true,
  customs_clearance: true,
  warehouse_capacity_tons: 2500
});

CREATE (can_cargo:CargoTerminal {
  terminal_id: "CAN-T1",
  airport_code: "CAN",
  name: "白云机场货运站",
  area_sqm: 45000,
  cold_storage: true,
  dangerous_goods_certified: false,
  customs_clearance: true,
  warehouse_capacity_tons: 1800
});

CREATE (hkg_cargo:CargoTerminal {
  terminal_id: "HKG-SHK",
  airport_code: "HKG",
  name: "超级一号货站",
  area_sqm: 130000,
  cold_storage: true,
  dangerous_goods_certified: true,
  customs_clearance: true,
  warehouse_capacity_tons: 5000
});

CREATE (sin_cargo:CargoTerminal {
  terminal_id: "SIN-CT1",
  airport_code: "SIN",
  name: "樟宜机场货运站",
  area_sqm: 55000,
  cold_storage: true,
  dangerous_goods_certified: true,
  customs_clearance: true,
  warehouse_capacity_tons: 2200
});

// ========================================
// 3. 创建航空公司节点
// ========================================

CREATE (ca:Airline {
  iata_code: "CA",
  name: "中国国际航空",
  country: "中国",
  cargo_fleet_size: 15,
  hub_airports: ["PEK", "CTU"]
});

CREATE (mu:Airline {
  iata_code: "MU",
  name: "中国东方航空",
  country: "中国",
  cargo_fleet_size: 12,
  hub_airports: ["PVG", "KMG"]
});

CREATE (cz:Airline {
  iata_code: "CZ",
  name: "中国南方航空",
  country: "中国",
  cargo_fleet_size: 10,
  hub_airports: ["CAN", "URU"]
});

CREATE (cx:Airline {
  iata_code: "CX",
  name: "国泰航空",
  country: "中国香港",
  cargo_fleet_size: 8,
  hub_airports: ["HKG"]
});

CREATE (sq:Airline {
  iata_code: "SQ",
  name: "新加坡航空",
  country: "新加坡",
  cargo_fleet_size: 7,
  hub_airports: ["SIN"]
});

// ========================================
// 4. 创建货物类型节点
// ========================================

CREATE (electronics:CargoType {
  type_id: "ELEC",
  category: "电子产品",
  requires_temperature_control: false,
  fragile: true,
  customs_hs_code: "8517",
  avg_value_per_kg_usd: 250
});

CREATE (pharma:CargoType {
  type_id: "PHAR",
  category: "医药品",
  requires_temperature_control: true,
  temp_range_celsius: "2-8",
  fragile: true,
  customs_hs_code: "3004",
  avg_value_per_kg_usd: 800
});

CREATE (perishable:CargoType {
  type_id: "PRSH",
  category: "生鲜食品",
  requires_temperature_control: true,
  temp_range_celsius: "-18-4",
  fragile: false,
  customs_hs_code: "0304",
  avg_value_per_kg_usd: 30
});

CREATE (machinery:CargoType {
  type_id: "MACH",
  category: "机械设备",
  requires_temperature_control: false,
  fragile: false,
  customs_hs_code: "8479",
  avg_value_per_kg_usd: 50
});

CREATE (textiles:CargoType {
  type_id: "TEXT",
  category: "纺织品",
  requires_temperature_control: false,
  fragile: false,
  customs_hs_code: "6204",
  avg_value_per_kg_usd: 15
});

// ========================================
// 5. 创建具体货物运单节点
// ========================================

CREATE (awb001:Shipment {
  awb_number: "999-12345678",
  shipper_name: "深圳华为技术有限公司",
  consignee_name: "Apple Inc.",
  origin: "PVG",
  destination: "LAX",
  cargo_type: "ELEC",
  weight_kg: 500,
  volume_cbm: 2.5,
  declared_value_usd: 125000,
  status: "在途",
  created_at: "2026-08-03T08:00:00Z",
  scheduled_departure: "2026-08-03T14:00:00Z",
  scheduled_arrival: "2026-08-03T10:00:00-08:00"
});

CREATE (awb002:Shipment {
  awb_number: "999-12345679",
  shipper_name: "上海医药集团",
  consignee_name: "WHO Europe",
  origin: "PVG",
  destination: "FRA",
  cargo_type: "PHAR",
  weight_kg: 200,
  volume_cbm: 1.2,
  declared_value_usd: 160000,
  status: "待装机",
  created_at: "2026-08-03T06:00:00Z",
  scheduled_departure: "2026-08-04T01:00:00Z",
  scheduled_arrival: "2026-08-04T06:00:00+02:00"
});

CREATE (awb003:Shipment {
  awb_number: "160-23456789",
  shipper_name: "广州冷链物流",
  consignee_name: "Singapore Food Corp",
  origin: "CAN",
  destination: "SIN",
  cargo_type: "PRSH",
  weight_kg: 1200,
  volume_cbm: 8.0,
  declared_value_usd: 36000,
  status: "已清关",
  created_at: "2026-08-02T10:00:00Z",
  scheduled_departure: "2026-08-03T11:00:00Z",
  scheduled_arrival: "2026-08-03T15:00:00+08:00"
});

CREATE (awb004:Shipment {
  awb_number: "180-34567890",
  shipper_name: "北京机械进出口",
  consignee_name: "NY Trading LLC",
  origin: "PEK",
  destination: "JFK",
  cargo_type: "MACH",
  weight_kg: 3500,
  volume_cbm: 25.0,
  declared_value_usd: 175000,
  status: "仓储中",
  created_at: "2026-08-01T14:00:00Z",
  scheduled_departure: "2026-08-05T02:00:00Z",
  scheduled_arrival: "2026-08-05T06:00:00-05:00"
});

// ========================================
// 6. 创建关系：机场 - 货运站
// ========================================

MATCH (pek:Airport {code: "PEK"}), (pek_cargo:CargoTerminal {terminal_id: "PEK-T1"})
CREATE (pek)-[:HAS_TERMINAL {distance_meters: 500}]->(pek_cargo);

MATCH (pvg:Airport {code: "PVG"}), (pvg_cargo:CargoTerminal {terminal_id: "PVG-T1"})
CREATE (pvg)-[:HAS_TERMINAL {distance_meters: 800}]->(pvg_cargo);

MATCH (can:Airport {code: "CAN"}), (can_cargo:CargoTerminal {terminal_id: "CAN-T1"})
CREATE (can)-[:HAS_TERMINAL {distance_meters: 600}]->(can_cargo);

MATCH (hkg:Airport {code: "HKG"}), (hkg_cargo:CargoTerminal {terminal_id: "HKG-SHK"})
CREATE (hkg)-[:HAS_TERMINAL {distance_meters: 300}]->(hkg_cargo);

MATCH (sin:Airport {code: "SIN"}), (sin_cargo:CargoTerminal {terminal_id: "SIN-CT1"})
CREATE (sin)-[:HAS_TERMINAL {distance_meters: 400}]->(sin_cargo);

// ========================================
// 7. 创建关系：航线网络
// ========================================

// 中国国航航线
MATCH (pek:Airport {code: "PEK"}), (pvg:Airport {code: "PVG"})
CREATE (pek)-[:ROUTE {
  airline: "CA",
  flight_number: "CA1501",
  frequency_per_week: 7,
  flight_time_hours: 2.5,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(pvg);

MATCH (pek:Airport {code: "PEK"}), (lax:Airport {code: "LAX"})
CREATE (pek)-[:ROUTE {
  airline: "CA",
  flight_number: "CA983",
  frequency_per_week: 3,
  flight_time_hours: 12.0,
  aircraft_type: "B747F",
  max_cargo_tons: 120
}]->(lax);

MATCH (pek:Airport {code: "PEK"}), (fra:Airport {code: "FRA"})
CREATE (pek)-[:ROUTE {
  airline: "CA",
  flight_number: "CA965",
  frequency_per_week: 5,
  flight_time_hours: 10.5,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(fra);

// 东航航线
MATCH (pvg:Airport {code: "PVG"}), (lax:Airport {code: "LAX"})
CREATE (pvg)-[:ROUTE {
  airline: "MU",
  flight_number: "MU577",
  frequency_per_week: 7,
  flight_time_hours: 11.5,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(lax);

MATCH (pvg:Airport {code: "PVG"}), (fra:Airport {code: "FRA"})
CREATE (pvg)-[:ROUTE {
  airline: "MU",
  flight_number: "MU219",
  frequency_per_week: 4,
  flight_time_hours: 11.0,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(fra);

MATCH (pvg:Airport {code: "PVG"}), (sin:Airport {code: "SIN"})
CREATE (pvg)-[:ROUTE {
  airline: "MU",
  flight_number: "MU545",
  frequency_per_week: 14,
  flight_time_hours: 5.5,
  aircraft_type: "A330F",
  max_cargo_tons: 60
}]->(sin);

// 南航航线
MATCH (can:Airport {code: "CAN"}), (sin:Airport {code: "SIN"})
CREATE (can)-[:ROUTE {
  airline: "CZ",
  flight_number: "CZ351",
  frequency_per_week: 10,
  flight_time_hours: 4.0,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(sin);

MATCH (can:Airport {code: "CAN"}), (lax:Airport {code: "LAX"})
CREATE (can)-[:ROUTE {
  airline: "CZ",
  flight_number: "CZ327",
  frequency_per_week: 3,
  flight_time_hours: 13.0,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(lax);

// 国泰航线
MATCH (hkg:Airport {code: "HKG"}), (lax:Airport {code: "LAX"})
CREATE (hkg)-[:ROUTE {
  airline: "CX",
  flight_number: "CX880",
  frequency_per_week: 7,
  flight_time_hours: 12.5,
  aircraft_type: "B747F",
  max_cargo_tons: 120
}]->(lax);

MATCH (hkg:Airport {code: "HKG"}), (jfk:Airport {code: "JFK"})
CREATE (hkg)-[:ROUTE {
  airline: "CX",
  flight_number: "CX840",
  frequency_per_week: 5,
  flight_time_hours: 15.0,
  aircraft_type: "B747F",
  max_cargo_tons: 120
}]->(jfk);

// 新航航线
MATCH (sin:Airport {code: "SIN"}), (lax:Airport {code: "LAX"})
CREATE (sin)-[:ROUTE {
  airline: "SQ",
  flight_number: "SQ37",
  frequency_per_week: 7,
  flight_time_hours: 17.5,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(lax);

MATCH (sin:Airport {code: "SIN"}), (fra:Airport {code: "FRA"})
CREATE (sin)-[:ROUTE {
  airline: "SQ",
  flight_number: "SQ326",
  frequency_per_week: 7,
  flight_time_hours: 12.5,
  aircraft_type: "B777F",
  max_cargo_tons: 100
}]->(fra);

// ========================================
// 8. 创建关系：货物运输流程
// ========================================

// AWB001: PVG -> LAX (华为出口到苹果)
MATCH (awb:Shipment {awb_number: "999-12345678"}), (pvg_cargo:CargoTerminal {terminal_id: "PVG-T1"})
CREATE (awb)-[:STORED_AT {
  check_in_time: "2026-08-03T08:30:00Z",
  warehouse_location: "A-101",
  handler: "张明"
}]->(pvg_cargo);

MATCH (awb:Shipment {awb_number: "999-12345678"}), (type:CargoType {type_id: "ELEC"})
CREATE (awb)-[:IS_TYPE]->(type);

// AWB002: PVG -> FRA (医药品)
MATCH (awb:Shipment {awb_number: "999-12345679"}), (pvg_cargo:CargoTerminal {terminal_id: "PVG-T1"})
CREATE (awb)-[:STORED_AT {
  check_in_time: "2026-08-03T06:15:00Z",
  warehouse_location: "COLD-05",
  handler: "李芳",
  temperature_celsius: 5.2
}]->(pvg_cargo);

MATCH (awb:Shipment {awb_number: "999-12345679"}), (type:CargoType {type_id: "PHAR"})
CREATE (awb)-[:IS_TYPE]->(type);

// AWB003: CAN -> SIN (生鲜)
MATCH (awb:Shipment {awb_number: "160-23456789"}), (can_cargo:CargoTerminal {terminal_id: "CAN-T1"})
CREATE (awb)-[:STORED_AT {
  check_in_time: "2026-08-02T11:00:00Z",
  warehouse_location: "COLD-12",
  handler: "王强",
  temperature_celsius: -2.5
}]->(can_cargo);

MATCH (awb:Shipment {awb_number: "160-23456789"}), (type:CargoType {type_id: "PRSH"})
CREATE (awb)-[:IS_TYPE]->(type);

// AWB004: PEK -> JFK (机械)
MATCH (awb:Shipment {awb_number: "180-34567890"}), (pek_cargo:CargoTerminal {terminal_id: "PEK-T1"})
CREATE (awb)-[:STORED_AT {
  check_in_time: "2026-08-01T15:00:00Z",
  warehouse_location: "B-203",
  handler: "赵伟"
}]->(pek_cargo);

MATCH (awb:Shipment {awb_number: "180-34567890"}), (type:CargoType {type_id: "MACH"})
CREATE (awb)-[:IS_TYPE]->(type);

// ========================================
// 演示查询完成
// 查询示例见 air-cargo-queries.cypher
// ========================================
