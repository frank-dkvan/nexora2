# Nexora 2 研发指导文档

## Real-time Event-Driven Temporal Graph Intelligence Platform

版本：v1.1

------------------------------------------------------------------------

# 1. 产品定位

Nexora 2 是面向企业 AI Agent 时代的新一代实时动态图智能基础平台。

核心目标：

> 将企业持续产生的业务事件、设备事件、人员行为事件和系统变化事件，实时转化为持续演化的时态知识图谱（Temporal
> Knowledge Graph），为 AI Agent 提供企业级实时认知、分析和推理能力。

Nexora 2 不是传统知识图谱系统，也不是简单图数据库，而是：

    Event
      |
    Dynamic Graph
      |
    Temporal Knowledge
      |
    AI Reasoning

形成企业实时世界模型（Enterprise Real-Time World Model）。

------------------------------------------------------------------------

# 2. 总体设计原则

## 2.1 平台化设计

Nexora 2 是整体平台，不绑定单一基础技术。

采用：

    80% 成熟商业友好开源技术
    +
    20% Nexora 自研核心能力

原则：

-   不重复建设 Kafka 等基础设施；
-   不重复建设通用流计算能力；
-   聚焦事件理解、动态图演化和智能推理。

------------------------------------------------------------------------

# 3. Nexora 2 总体架构

                             AI Agent Layer
                                  |
                                  |
                  Graph Intelligence & Reasoning API
                                  |
                                  |
                     Temporal Knowledge Graph
                                  |
                                  |
                        Nexora Graph Engine
                                  |
           ------------------------------------------------
           |                     |                        |
     Dynamic Graph Engine   Temporal Engine      Graph Reasoning
           |
           |
    ========================================================
                     Nexora Runtime Platform
    ========================================================
           |
           |
     -------------------------------------------------------
     |                       |                             |
     Event Stream Module   Semantic Event Module     Data Connector
     (RisingWave)          (Nexora自研)              (CDC/MQTT/API)
     |
     |
     Kafka / Redpanda / Pulsar
     |
     |
    Enterprise Systems / IoT / Applications

------------------------------------------------------------------------

# 4. 核心模块设计

# 4.1 Event Stream Processing Module

## 定位

基于 RisingWave 的实时事件流处理模块。

英文：

Nexora Event Stream Engine powered by RisingWave

------------------------------------------------------------------------

## 主要职责

### 事件接入

支持：

-   Kafka
-   MQTT
-   CDC
-   API

事件示例：

    CargoArrived
    CargoLoaded
    EquipmentFailure
    EmployeeAccess

------------------------------------------------------------------------

### 实时流计算

包括：

-   Window Calculation
-   Stream Join
-   Aggregation
-   Materialized View
-   Real-time State Calculation

------------------------------------------------------------------------

### 模块边界

RisingWave负责：

    理解事件流

不负责：

    理解企业业务语义

------------------------------------------------------------------------

# 4.2 Semantic Event Engine

## Nexora核心自研模块

作用：

将技术事件转换为业务语义事件。

例如：

数据库：

    UPDATE cargo_status=50

转换：

    CargoArrived

------------------------------------------------------------------------

能力：

-   Event Ontology
-   Event Mapping
-   Schema Evolution
-   Business Event Model

------------------------------------------------------------------------

# 4.3 Graph Mutation Engine

## Nexora核心自研模块

作用：

将业务事件转化为图变化。

例如：

事件：

    CargoLoaded

生成：

    ADD EDGE

    Cargo
     |
    LOADED_ON
     |
    Flight

支持：

-   Add Node
-   Update Property
-   Add Relationship
-   Remove Relationship
-   Temporal Mutation

------------------------------------------------------------------------

# 4.4 Temporal Graph Engine

## Nexora核心技术壁垒

目标：

构建持续演化的时态知识图谱。

支持：

## Bi-temporal Model

两个时间维度：

### Event Time

事件实际发生时间。

### System Time

系统感知时间。

能力：

-   历史状态保存
-   状态演化追踪
-   Time Travel Query
-   事实来源追踪

------------------------------------------------------------------------

# 4.5 Graph Stream Intelligence Engine

## Nexora核心自研模块

负责实时动态图分析。

能力：

## Continuous Query

持续监控图状态。

例如：

    员工访问敏感系统

    +

    异常时间

    +

    异常地点

    => 风险事件

------------------------------------------------------------------------

## Standing Query

长期运行的图查询。

类似：

Quine 的核心思想。

------------------------------------------------------------------------

## Pattern Detection

实时发现：

-   风险模式
-   业务异常
-   影响链路

------------------------------------------------------------------------

# 4.6 AI Graph Reasoning Layer

为 AI Agent 提供：

-   Graph RAG
-   Reasoning Path
-   Cause Analysis
-   Impact Analysis

示例：

问题：

    为什么航班延误？

推理：

    Flight Delay

    ↓

    Equipment Failure

    ↓

    Warehouse Congestion

    ↓

    Cargo Impact

------------------------------------------------------------------------

# 5. RisingWave 在 Nexora 2 中的定位

RisingWave 是 Nexora 2 的一个模块。

不是外部系统。

产品层：

    Nexora 2 Platform

        |
        |
    Event Stream Module

        |
        |
    RisingWave

------------------------------------------------------------------------

职责：

-   实时事件计算
-   状态维护
-   流式SQL
-   实时物化视图
-   CDC处理

不承担：

-   图存储
-   图推理
-   业务语义理解
-   时态知识建模

------------------------------------------------------------------------

# 6. CloudEvents事件标准

Nexora 2 采用 CloudEvents 作为基础事件协议。

CloudEvents负责：

-   事件格式统一
-   事件来源
-   唯一ID
-   时间信息
-   事件类型

示例：

``` json
{
 "specversion":"1.0",
 "id":"evt001",
 "source":"/enterprise/system",
 "type":"com.company.cargo.arrived",
 "subject":"AWB123456",
 "time":"2026-07-26T10:20:00Z",
 "data":{
    "cargoId":"AWB123456"
 }
}
```

------------------------------------------------------------------------

# 7. Nexora 三层事件模型

    Layer 1

    CloudEvents

    技术事件协议


            |


    Layer 2

    Nexora Semantic Event

    业务语义事件


            |


    Layer 3

    Graph Mutation Event

    图变化事件

------------------------------------------------------------------------

# 8. 核心数据流

    Enterprise Systems
            |
            |
    Kafka / MQTT / CDC
            |
            |
    CloudEvents
            |
            |
    RisingWave Event Stream Module
            |
            |
    Semantic Event Engine
            |
            |
    Graph Mutation Engine
            |
            |
    Temporal Knowledge Graph
            |
            |
    AI Agent

------------------------------------------------------------------------

# 9. 技术选型建议

  能力       技术
  ---------- ---------------------------------
  事件总线   Kafka / Redpanda
  流计算     RisingWave
  CDC        Debezium
  事件协议   CloudEvents
  数据湖     Apache Iceberg
  图存储     Kuzu / LadybugDB / Nebula Graph
  API        Rust + Axum
  核心引擎   Rust
  查询       Cypher / GQL
  Tracing    OpenTelemetry
  AI接口     MCP

------------------------------------------------------------------------

# 10. 研发路线

## Phase 1：MVP

目标：

验证：

    Event → Graph → AI

实现：

-   CloudEvents接入
-   RisingWave集成
-   Semantic Event Engine
-   Graph Mutation
-   Temporal Graph
-   Query API

------------------------------------------------------------------------

## Phase 2：产品化

增加：

-   分布式部署
-   高可用
-   多租户
-   安全体系
-   Graph Stream Compute
-   企业Ontology

------------------------------------------------------------------------

## Phase 3：平台化

目标：

形成：

    Enterprise Real-Time World Model Platform

应用：

-   企业数字孪生
-   AI Agent基础设施
-   决策智能平台

------------------------------------------------------------------------

# 11. Nexora 2核心技术壁垒

不竞争：

Kafka：

事件传输

RisingWave：

事件计算

Neo4j：

图查询

Nexora核心：

    理解事件如何改变企业世界

核心能力：

1.  Semantic Event Engine
2.  Temporal Graph Runtime
3.  Graph Mutation Engine
4.  Real-time Graph Reasoning

------------------------------------------------------------------------

# 12. 产品定位

英文：

Nexora 2 is a real-time event-driven temporal graph intelligence
platform that transforms enterprise events into an evolving knowledge
graph for AI agents.

中文：

Nexora 2
是一个事件驱动的实时动态图智能平台，将企业事件持续转化为不断演化的时态知识图谱，为下一代企业
AI Agent 提供实时认知和推理能力。
