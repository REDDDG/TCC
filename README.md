# TCC 分布式事务框架

基于 **TCC（Try-Confirm-Cancel）** 模式的分布式事务框架，使用 **gRPC** 进行进程间通信，**Redis Lua** 作为热路径原子操作引擎，**Kafka** 异步将终态数据刷入 **MySQL**，实现高性能、最终一致性的分布式事务协调。

## 架构概览

```
                    ┌─────────────┐
                    │   Gin :8080  │  ← HTTP API + Web 前端
                    │ Coordinator  │
                    │  gRPC :9090  │  ← TCC 事务编排
                    └──────┬──────┘
          Try/Confirm/Cancel │ gRPC
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ Inventory     │   │ Order         │   │ Points        │
│ :8082         │   │ :8081         │   │ :8083         │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────┐
│                  Redis (热路径)                        │
│  Lua 脚本原子操作: Try 冻结 → Confirm 扣减 + 版本递增  │
└───────────────────────┬───────────────────────────────┘
                        │ Kafka (异步)
                        ▼
┌───────────────────────────────────────────────────────┐
│              MySQL (持久化)                              │
└───────────────────────────────────────────────────────┘
```

## TCC 三阶段流程

| 阶段 | 操作 | Redis 行为 |
|------|------|-----------|
| **Try** | 预留资源 | 检查 `balance - frozen - qty >= 0`，通过则 `INCRBY frozen`，不扣减余额 |
| **Confirm** | 确认提交 | 原子操作：`DECRBY balance` + `DECRBY frozen` + `INCR version`，结果通过 Kafka 异步刷入 MySQL |
| **Cancel** | 补偿回滚 | 仅 `DECRBY frozen`（解冻），不触碰余额 |

## 核心亮点

### 1. 热路径 Redis Lua 原子化

Try、Confirm、Cancel 全部使用 Lua 脚本在 Redis 内单次往返完成多步操作，消除网络往返开销和并发竞争。

```
单次事务路径: 协调器 → gRPC → 分支服务 → Redis Lua (1 次 RTT) → 返回
```

### 2. Kafka 异步刷入 MySQL

事务终态（Completed/Failed）不阻塞热路径——Confirm 完成后立即返回，Kafka Consumer 异步消费并写入 MySQL。**事务平均延迟从 ~30ms 降至 ~15ms，性能提升约 50%。**

```
热路径:  Redis (同步，~1ms)
冷路径:  Kafka → MySQL (异步，不阻塞响应)
```

### 3. Frozen 冻结金额模式

Try 阶段不直接扣减余额，而是增加 `frozen`（未确认扣款总额）。每次 Try 检查：

```
current_balance - total_frozen - pending_deduction >= 0
```

这保证了在并发 Try 场景下，已冻结但未确认的金额不会被重复分配。Cancel 仅解冻——余额从未被实际扣除，避免补偿时的不一致。

### 4. 版本号追踪

每个资源（商品/用户积分）维护独立的单调递增版本号，在 **Confirm 阶段递增**，随 Kafka 消息传递到 MySQL 消费者同步写入：

```sql
UPDATE inventory_stock
SET total = total - ?, version = ?, updated_at = NOW()
WHERE product_id = ?
```

版本号跟随 Confirm 写入 MySQL，保证每条 Kafka 消息的消费结果可追溯——`version` 字段记录了该资源被确认修改的次数，便于排查数据一致性问题。

### 5. 超时自动取消

后台 Scanner 定期扫描超时且未终态的事务，自动执行 **Cancel** 释放已冻结资源：

- `StatusTrying` → 对所有已 Try 的分支发起 Cancel
所有操作均为幂等——Cancel 允许多次调用，保障资源不会因协调器宕机或网络中断而永久锁定。

### 6. 三服务独立部署

Inventory（库存）、Order（订单）、Points（积分）作为独立的 gRPC 服务运行，各自持有 Redis 连接。协调器通过 gRPC 调度三阶段流程，服务间无直接耦合。

## 项目结构

```
TCC/
├── api/proto/                  # Protocol Buffers 定义
│   ├── coordinator/            # 协调器接口 (Begin/Commit/Cancel/GetStatus)
│   └── branch/                 # 分支服务接口 (Try/Confirm/Cancel)
├── cmd/                        # 启动入口
│   ├── coordinator/            # 协调器 (gRPC :9090 + Gin :8080)
│   ├── inventory-service/      # 库存服务 (:8082)
│   ├── order-service/          # 订单服务 (:8081)
│   └── points-service/         # 积分服务 (:8083)
├── internal/
│   ├── coordinator/            # TCC 状态机编排 + HTTP API
│   ├── branch/                 # 分支服务实现 (inventory/order/points)
│   ├── repository/             # 持久化层
│   │   ├── redis.go            # 热路径 Lua 脚本 + Try/Confirm/Cancel
│   │   ├── kafka.go            # Kafka Producer/Consumer + 乐观锁 MySQL 写入
│   │   ├── mysql.go            # MySQL CRUD (事务记录持久化)
│   │   └── repository.go       # Repository 接口抽象
│   ├── model/                  # 共享数据类型
│   ├── recoverer/              # 超时扫描器 + 恢复器
│   └── middleware/             # Gin 中间件
├── web/                        # 前端页面
├── mysql.sql                   # 建表 DDL
└── go.mod
```

## 快速开始

### 前置依赖

- Go 1.26+
- Redis 7+
- Kafka 3.x
- MySQL 8.0+

### 启动

```bash
# 1. 初始化数据库
mysql -u root -p < mysql.sql

# 2. 启动分支服务
go run cmd/inventory-service/main.go &
go run cmd/order-service/main.go &
go run cmd/points-service/main.go &

# 3. 启动协调器
go run cmd/coordinator/main.go
```

### 发起事务

```bash
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "participants": [
      {"service_name": "InventoryService", "resource_data": "product-001", "address": "localhost:8082", "value": 1},
      {"service_name": "PointsService",   "resource_data": "{\"user_id\":\"user-1\"}", "address": "localhost:8083", "value": 1}
    ],
    "timeout": 30
  }'
```

## 技术栈

| 组件 | 技术 |
|------|------|
| 语言 | Go 1.26 |
| 通信协议 | gRPC + Protocol Buffers |
| 热路径缓存 | Redis 7 + Lua 脚本 |
| 消息队列 | Kafka (Sarama) |
| 持久化 | MySQL 8.0 |
| HTTP 层 | Gin |
| 前端 | 原生 HTML/JS |