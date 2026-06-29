package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"tcc/internal/model"

	"github.com/redis/go-redis/v9"
)

// luaTry 实现 frozen 模式：Try 阶段只冻结金额、不递增 version。
//
//	KEYS[1] = balance key  (inventory:{pid} / points:{uid})
//	KEYS[2] = frozen key   (inv:frozen:{pid} / pts:frozen:{uid})
//	KEYS[3] = try key      (try:inv:{bid} / try:pts:{bid})
//	ARGV[1] = qty
//	返回: 1 成功; 0 余额不足
//
//	检查逻辑: current - frozen - qty >= 0
const luaTry = `
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local frozen = tonumber(redis.call('GET', KEYS[2]) or '0')
local qty = tonumber(ARGV[1])

if current - frozen - qty >= 0 then
    redis.call('INCRBY', KEYS[2], qty)
    redis.call('SET', KEYS[3], qty, 'EX', 300)
    return 1
end
return 0
`

// luaConfirm 确认阶段：扣减余额、解冻、递增 version，返回 "qty:new_version"。
//
//	KEYS[1] = balance key
//	KEYS[2] = frozen key
//	KEYS[3] = version key
//	KEYS[4] = try key
const luaConfirm = `
local qty_str = redis.call('GET', KEYS[4])
if not qty_str then
    return false
end
local qty = tonumber(qty_str)

local version = redis.call('INCR', KEYS[3])
redis.call('DECRBY', KEYS[1], qty)
redis.call('DECRBY', KEYS[2], qty)
redis.call('DEL', KEYS[4])
return qty .. ':' .. version
`

// luaCancel 取消阶段：只解冻冻结金额，不触碰余额（Try 阶段未实际扣减）。
//
//	KEYS[1] = frozen key
//	KEYS[2] = try key
const luaCancel = `
local qty_str = redis.call('GET', KEYS[2])
if not qty_str then
    return 0
end
local qty = tonumber(qty_str)

redis.call('DECRBY', KEYS[1], qty)
redis.call('DEL', KEYS[2])
return qty
`

// Redis 数据结构：
//
//	seq:branch_id              → String (全局自增分支 ID)
//	br_xid:{branch_id}         → String (branch_id → xid 反向映射)
//	tx:all                     → Set   (全部 XID，用于扫描/列表)
//	tx:{xid}                   → Hash  {status, timeout, retry_count, create_time, update_time}
//	tx:{xid}:branches          → Set   (该事务的所有 branch_id)
//	tx:{xid}:br:{branch_id}    → Hash  {service_name, address, resource_data, status, try_data, create_time, update_time}
//
//	--- Inventory (frozen + version 乐观锁) ---
//	inventory:{product_id}     → String 当前可用库存
//	inv:frozen:{product_id}    → String 未确认的冻结扣减总额
//	inv:version:{product_id}   → String 版本号（每次 Confirm 自增，用于 Kafka 消费者乐观锁）
//	try:inv:{branch_id}        → String qty (TTL 300s)
//
//	--- Points (frozen + version 乐观锁) ---
//	points:{user_id}           → String 当前可用积分
//	pts:frozen:{user_id}       → String 未确认的冻结积分总额
//	pts:version:{user_id}      → String 版本号（每次 Confirm 自增）
//	try:pts:{branch_id}        → String qty (TTL 300s)

type RedisRepository struct {
	rdb      *redis.Client
	producer *KafkaProducer
}

func NewRedisRepository(rdb *redis.Client, producer *KafkaProducer) *RedisRepository {
	return &RedisRepository{rdb: rdb, producer: producer}
}

// --- Inventory ---

func (r *RedisRepository) InventoryTry(ctx context.Context, branchId int64, value int64, productId string) error {
	key := "inventory:" + productId
	frozenKey := "inv:frozen:" + productId
	tryKey := fmt.Sprintf("try:inv:%d", branchId)

	result, err := r.rdb.Eval(ctx, luaTry, []string{key, frozenKey, tryKey}, value).Result()
	if err != nil {
		return fmt.Errorf("redis try: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("insufficient inventory for %s", productId)
	}
	return nil
}

func (r *RedisRepository) InventoryConfirm(ctx context.Context, branchId int64, productId string) error {
	key := "inventory:" + productId
	frozenKey := "inv:frozen:" + productId
	versionKey := "inv:version:" + productId
	tryKey := fmt.Sprintf("try:inv:%d", branchId)

	result, err := r.rdb.Eval(ctx, luaConfirm, []string{key, frozenKey, versionKey, tryKey}).Result()
	if err != nil {
		return fmt.Errorf("redis confirm: %w", err)
	}
	if result == nil {
		return fmt.Errorf("try record not found for branch %d", branchId)
	}

	parts := strings.SplitN(result.(string), ":", 2)
	qty := parts[0]
	version, _ := strconv.Atoi(parts[1])

	if err := r.producer.Send(ctx, SyncMessage{
		BranchID:   branchId,
		Service:    "inventory",
		Phase:      "confirm",
		ResourceID: productId,
		Data:       qty,
		Version:    version,
	}); err != nil {
		return fmt.Errorf("kafka send: %w", err)
	}
	return nil
}

func (r *RedisRepository) InventoryCancel(ctx context.Context, branchId int64, productId string) error {
	frozenKey := "inv:frozen:" + productId
	tryKey := fmt.Sprintf("try:inv:%d", branchId)

	result, err := r.rdb.Eval(ctx, luaCancel, []string{frozenKey, tryKey}).Result()
	if err != nil {
		return fmt.Errorf("redis cancel: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("try record not found for branch %d", branchId)
	}
	return nil
}

// --- Points ---

func (r *RedisRepository) PointsTry(ctx context.Context, branchId int64, value int64, account model.PointsAccount) error {
	key := "points:" + account.UserID
	frozenKey := "pts:frozen:" + account.UserID
	tryKey := fmt.Sprintf("try:pts:%d", branchId)

	result, err := r.rdb.Eval(ctx, luaTry, []string{key, frozenKey, tryKey}, value).Result()
	if err != nil {
		return fmt.Errorf("redis try: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("insufficient points for %s", account.UserID)
	}
	return nil
}

func (r *RedisRepository) PointsConfirm(ctx context.Context, branchId int64, account model.PointsAccount) error {
	key := "points:" + account.UserID
	frozenKey := "pts:frozen:" + account.UserID
	versionKey := "pts:version:" + account.UserID
	tryKey := fmt.Sprintf("try:pts:%d", branchId)

	result, err := r.rdb.Eval(ctx, luaConfirm, []string{key, frozenKey, versionKey, tryKey}).Result()
	if err != nil {
		return fmt.Errorf("redis confirm: %w", err)
	}
	if result == nil {
		return fmt.Errorf("try record not found for branch %d", branchId)
	}

	parts := strings.SplitN(result.(string), ":", 2)
	qty := parts[0]
	version, _ := strconv.Atoi(parts[1])

	if err := r.producer.Send(ctx, SyncMessage{
		BranchID:   branchId,
		Service:    "points",
		Phase:      "confirm",
		ResourceID: account.UserID,
		Data:       qty,
		Version:    version,
	}); err != nil {
		return fmt.Errorf("kafka send: %w", err)
	}
	return nil
}

func (r *RedisRepository) PointsCancel(ctx context.Context, branchId int64, account model.PointsAccount) error {
	frozenKey := "pts:frozen:" + account.UserID
	tryKey := fmt.Sprintf("try:pts:%d", branchId)

	result, err := r.rdb.Eval(ctx, luaCancel, []string{frozenKey, tryKey}).Result()
	if err != nil {
		return fmt.Errorf("redis cancel: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("try record not found for branch %d", branchId)
	}
	return nil
}

// --- Order (Try/Confirm/Cancel 都发 Kafka) ---

func (r *RedisRepository) OrderTry(ctx context.Context, branchId int64, xid string, order model.Order) error {
	key := fmt.Sprintf("order:%s", xid)
	r.rdb.HSet(ctx, key,
		"user_id", order.UserID,
		"product_id", order.ProductID,
		"quantity", order.Quantity,
		"amount", order.Amount,
		"status", 0,
	)
	data, _ := json.Marshal(map[string]interface{}{
		"user_id":    order.UserID,
		"product_id": order.ProductID,
		"quantity":   order.Quantity,
		"amount":     order.Amount,
	})
	return r.producer.Send(ctx, SyncMessage{
		XID:      xid,
		BranchID: branchId,
		Service:  "order",
		Phase:    "try",
		Data:     string(data),
	})
}

func (r *RedisRepository) OrderConfirm(ctx context.Context, branchId int64, xid string, order model.Order) error {
	key := fmt.Sprintf("order:%s", xid)
	r.rdb.HSet(ctx, key, "status", 1)
	return r.producer.Send(ctx, SyncMessage{
		XID:      xid,
		BranchID: branchId,
		Service:  "order",
		Phase:    "confirm",
	})
}

func (r *RedisRepository) OrderCancel(ctx context.Context, branchId int64, xid string, order model.Order) error {
	key := fmt.Sprintf("order:%s", xid)
	r.rdb.HSet(ctx, key, "status", 2)
	return r.producer.Send(ctx, SyncMessage{
		XID:      xid,
		BranchID: branchId,
		Service:  "order",
		Phase:    "cancel",
	})
}

// --- Transaction CRUD (纯 Redis) ---

func (r *RedisRepository) CreateTransaction(ctx context.Context, tx *model.Transaction) error {
	now := time.Now().Format(time.RFC3339Nano)

	// 为每个分支分配全局唯一 ID
	for _, br := range tx.Branches {
		id, err := r.rdb.Incr(ctx, "seq:branch_id").Result()
		if err != nil {
			return fmt.Errorf("incr seq:branch_id: %w", err)
		}
		br.BranchID = id
		brKey := fmt.Sprintf("tx:%s:br:%d", tx.XID, id)
		pipe := r.rdb.Pipeline()
		pipe.HSet(ctx, brKey,
			"service_name", br.ServiceName,
			"address", br.Address,
			"resource_data", br.ResourceData,
			"status", string(br.Status),
			"try_data", br.TryData,
			"create_time", now,
			"update_time", now,
		)
		pipe.Set(ctx, fmt.Sprintf("br_xid:%d", id), tx.XID, time.Hour)
		pipe.SAdd(ctx, fmt.Sprintf("tx:%s:branches", tx.XID), id)
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("create branch redis: %w", err)
		}
	}

	// 写全局事务
	key := "tx:" + tx.XID
	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, key,
		"status", string(tx.Status),
		"timeout", tx.Timeout,
		"retry_count", tx.RetryCount,
		"create_time", now,
		"update_time", now,
	)
	pipe.SAdd(ctx, "tx:all", tx.XID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("create tx redis: %w", err)
	}
	return nil
}

func (r *RedisRepository) GetTransaction(ctx context.Context, xid string) (*model.Transaction, error) {
	key := "tx:" + xid
	vals, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall tx: %w", err)
	}
	if len(vals) == 0 {
		return nil, nil
	}

	tx := &model.Transaction{XID: xid}
	tx.Status = model.TxStatus(vals["status"])
	tx.Timeout, _ = strconv.Atoi(vals["timeout"])
	tx.RetryCount, _ = strconv.Atoi(vals["retry_count"])
	tx.CreateTime, _ = time.Parse(time.RFC3339Nano, vals["create_time"])
	tx.UpdateTime, _ = time.Parse(time.RFC3339Nano, vals["update_time"])

	// 读所有分支
	branchIDs, err := r.rdb.SMembers(ctx, fmt.Sprintf("tx:%s:branches", xid)).Result()
	if err != nil {
		return nil, fmt.Errorf("smembers branches: %w", err)
	}
	for _, idStr := range branchIDs {
		brKey := fmt.Sprintf("tx:%s:br:%s", xid, idStr)
		brVals, err := r.rdb.HGetAll(ctx, brKey).Result()
		if err != nil {
			continue
		}
		br := &model.BranchTransaction{}
		br.BranchID, _ = strconv.ParseInt(idStr, 10, 64)
		br.ServiceName = brVals["service_name"]
		br.Address = brVals["address"]
		br.ResourceData = brVals["resource_data"]
		br.Status = model.BranchStatus(brVals["status"])
		br.TryData = brVals["try_data"]
		br.CreateTime, _ = time.Parse(time.RFC3339Nano, brVals["create_time"])
		br.UpdateTime, _ = time.Parse(time.RFC3339Nano, brVals["update_time"])
		tx.Branches = append(tx.Branches, br)
	}
	return tx, nil
}

func (r *RedisRepository) GetBranchTransaction(ctx context.Context, id int64) (model.BranchStatus, error) {
	// 需要先找到该 branch 属于哪个 xid。用 scan 查找。
	// 优化：直接维护 branch_id → xid 映射
	xid, err := r.rdb.Get(ctx, fmt.Sprintf("br_xid:%d", id)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get br_xid: %w", err)
	}

	brKey := fmt.Sprintf("tx:%s:br:%d", xid, id)
	status, err := r.rdb.HGet(ctx, brKey, "status").Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("hget branch status: %w", err)
	}
	return model.BranchStatus(status), nil
}

func (r *RedisRepository) UpdateBranchTransaction(ctx context.Context, id int64, status model.BranchStatus) error {
	return r.UpdateBranchStatus(ctx, id, status)
}

func (r *RedisRepository) UpdateTransactionStatus(ctx context.Context, xid string, status model.TxStatus) error {
	key := "tx:" + xid
	now := time.Now().Format(time.RFC3339Nano)
	if err := r.rdb.HSet(ctx, key, "status", string(status), "update_time", now).Err(); err != nil {
		return fmt.Errorf("hset tx status: %w", err)
	}

	// 终态：异步刷入 MySQL
	if status == model.StatusCompleted || status == model.StatusFailed {
		tx, err := r.GetTransaction(ctx, xid)
		if err != nil || tx == nil {
			return err
		}
		data, _ := json.Marshal(tx)
		if err := r.producer.Send(ctx, SyncMessage{
			XID:     xid,
			Service: "transaction",
			Phase:   "complete",
			Data:    string(data),
		}); err != nil {
			return fmt.Errorf("kafka flush tx: %w", err)
		}
		// 设置 TTL，避免 Redis 堆积
		r.rdb.Expire(ctx, key, time.Hour)
		r.rdb.Expire(ctx, fmt.Sprintf("tx:%s:branches", xid), time.Hour)
		for _, br := range tx.Branches {
			brKey := fmt.Sprintf("tx:%s:br:%d", xid, br.BranchID)
			r.rdb.Expire(ctx, brKey, time.Hour)
		}
	}
	return nil
}

func (r *RedisRepository) UpdateBranchStatus(ctx context.Context, branchID int64, status model.BranchStatus) error {
	xid, err := r.rdb.Get(ctx, fmt.Sprintf("br_xid:%d", branchID)).Result()
	if err == redis.Nil {
		return fmt.Errorf("branch %d not found", branchID)
	}
	if err != nil {
		return fmt.Errorf("get br_xid: %w", err)
	}
	brKey := fmt.Sprintf("tx:%s:br:%d", xid, branchID)
	now := time.Now().Format(time.RFC3339Nano)
	return r.rdb.HSet(ctx, brKey, "status", string(status), "update_time", now).Err()
}

func (r *RedisRepository) FindTimeOutTransactions(ctx context.Context) ([]*model.Transaction, error) {
	var txs []*model.Transaction
	var cursor uint64
	for {
		keys, next, err := r.rdb.SScan(ctx, "tx:all", cursor, "*", 50).Result()
		if err != nil {
			return nil, fmt.Errorf("sscan tx:all: %w", err)
		}
		for _, xid := range keys {
			tx, err := r.GetTransaction(ctx, xid)
			if err != nil || tx == nil {
				continue
			}
			// 非终态且已超时
			if tx.Status != model.StatusCompleted && tx.Status != model.StatusFailed {
				if time.Since(tx.UpdateTime) > time.Duration(tx.Timeout)*time.Second {
					txs = append(txs, tx)
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return txs, nil
}

func (r *RedisRepository) ListTransactions(ctx context.Context) ([]*model.Transaction, error) {
	var txs []*model.Transaction
	var cursor uint64
	for {
		keys, next, err := r.rdb.SScan(ctx, "tx:all", cursor, "*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("sscan tx:all: %w", err)
		}
		for _, xid := range keys {
			tx, err := r.GetTransaction(ctx, xid)
			if err != nil || tx == nil {
				continue
			}
			txs = append(txs, tx)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return txs, nil
}

func (r *RedisRepository) ClearAllTransactions(ctx context.Context) error {
	// 删所有事务相关 key
	var cursor uint64
	for {
		keys, next, err := r.rdb.SScan(ctx, "tx:all", cursor, "*", 100).Result()
		if err != nil {
			return fmt.Errorf("sscan tx:all: %w", err)
		}
		for _, xid := range keys {
			tx, err := r.GetTransaction(ctx, xid)
			if err != nil || tx == nil {
				continue
			}
			pipe := r.rdb.Pipeline()
			pipe.Del(ctx, "tx:"+xid)
			pipe.Del(ctx, fmt.Sprintf("tx:%s:branches", xid))
			for _, br := range tx.Branches {
				pipe.Del(ctx, fmt.Sprintf("tx:%s:br:%d", xid, br.BranchID))
				pipe.Del(ctx, fmt.Sprintf("br_xid:%d", br.BranchID))
			}
			pipe.Exec(ctx)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	r.rdb.Del(ctx, "tx:all")
	r.rdb.Del(ctx, "seq:branch_id")
	return nil
}

func (r *RedisRepository) AddRetryCount(ctx context.Context, xid string) error {
	return r.rdb.HIncrBy(ctx, "tx:"+xid, "retry_count", 1).Err()
}

// Close 关闭底层资源。
func (r *RedisRepository) Close() {
	if r.producer != nil {
		r.producer.Close()
	}
}
