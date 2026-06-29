package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"tcc/internal/model"

	"github.com/redis/go-redis/v9"
)

const luaTryDecr = `
local key = KEYS[1]
local try_key = KEYS[2]
local qty = tonumber(ARGV[1])
local current = redis.call('GET', key)
if current and tonumber(current) >= qty then
    redis.call('DECRBY', key, qty)
    redis.call('SET', try_key, ARGV[1], 'EX', 300)
    return 1
end
return 0
`

// Redis 数据结构：
//
//	seq:branch_id              → String (全局自增分支 ID)
//	br_xid:{branch_id}         → String (branch_id → xid 反向映射)
//	tx:all                     → Set   (全部 XID，用于扫描/列表)
//	tx:{xid}                   → Hash  {status, timeout, retry_count, create_time, update_time}
//	tx:{xid}:branches          → Set   (该事务的所有 branch_id)
//	tx:{xid}:br:{branch_id}    → Hash  {service_name, address, resource_data, status, try_data, create_time, update_time}

type RedisRepository struct {
	mysql    *MySQLRepository
	rdb      *redis.Client
	producer *KafkaProducer
}

func NewRedisRepository(mysql *MySQLRepository, rdb *redis.Client, producer *KafkaProducer) *RedisRepository {
	return &RedisRepository{mysql: mysql, rdb: rdb, producer: producer}
}

// --- Inventory ---

func (r *RedisRepository) InventoryTry(ctx context.Context, branchId int64, xid string, productId string) error {
	key := "inventory:" + productId
	tryKey := fmt.Sprintf("try:inv:%d", branchId)
	result, err := r.rdb.Eval(ctx, luaTryDecr, []string{key, tryKey}, "1").Result()
	if err != nil {
		return fmt.Errorf("redis try: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("insufficient inventory for %s", productId)
	}
	return nil
}

func (r *RedisRepository) InventoryConfirm(ctx context.Context, branchId int64, xid string, productId string) error {
	tryKey := fmt.Sprintf("try:inv:%d", branchId)
	qty, err := r.rdb.Get(ctx, tryKey).Result()
	if err != nil {
		return fmt.Errorf("get try record: %w", err)
	}
	if err := r.producer.Send(ctx, SyncMessage{
		BranchID:   branchId,
		Service:    "inventory",
		Phase:      "confirm",
		ResourceID: productId,
		Data:       qty,
	}); err != nil {
		return fmt.Errorf("kafka send: %w", err)
	}
	r.rdb.Del(ctx, tryKey)
	return nil
}

func (r *RedisRepository) InventoryCancel(ctx context.Context, branchId int64, xid string, productId string) error {
	key := "inventory:" + productId
	tryKey := fmt.Sprintf("try:inv:%d", branchId)
	qty, err := r.rdb.Get(ctx, tryKey).Result()
	if err != nil {
		return fmt.Errorf("get try record for cancel: %w", err)
	}
	qtyInt, _ := strconv.ParseInt(qty, 10, 64)
	r.rdb.IncrBy(ctx, key, qtyInt)
	r.rdb.Del(ctx, tryKey)
	return nil
}

// --- Points ---

func (r *RedisRepository) PointsTry(ctx context.Context, branchId int64, xid string, account model.PointsAccount) error {
	key := "points:" + account.UserID
	tryKey := fmt.Sprintf("try:pts:%d", branchId)
	result, err := r.rdb.Eval(ctx, luaTryDecr, []string{key, tryKey}, "1").Result()
	if err != nil {
		return fmt.Errorf("redis try: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("insufficient points for %s", account.UserID)
	}
	return nil
}

func (r *RedisRepository) PointsConfirm(ctx context.Context, branchId int64, xid string, account model.PointsAccount) error {
	tryKey := fmt.Sprintf("try:pts:%d", branchId)
	qty, err := r.rdb.Get(ctx, tryKey).Result()
	if err != nil {
		return fmt.Errorf("get try record: %w", err)
	}
	if err := r.producer.Send(ctx, SyncMessage{
		BranchID:   branchId,
		Service:    "points",
		Phase:      "confirm",
		ResourceID: account.UserID,
		Data:       qty,
	}); err != nil {
		return fmt.Errorf("kafka send: %w", err)
	}
	r.rdb.Del(ctx, tryKey)
	return nil
}

func (r *RedisRepository) PointsCancel(ctx context.Context, branchId int64, xid string, account model.PointsAccount) error {
	key := "points:" + account.UserID
	tryKey := fmt.Sprintf("try:pts:%d", branchId)
	qty, err := r.rdb.Get(ctx, tryKey).Result()
	if err != nil {
		return fmt.Errorf("get try record for cancel: %w", err)
	}
	qtyInt, _ := strconv.ParseInt(qty, 10, 64)
	r.rdb.IncrBy(ctx, key, qtyInt)
	r.rdb.Del(ctx, tryKey)
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
