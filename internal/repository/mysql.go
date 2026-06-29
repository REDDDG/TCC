package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"tcc/internal/myTime"
	"time"

	"tcc/internal/model"

	_ "github.com/go-sql-driver/mysql"
)

// 状态常量映射：Go 字符串枚举 ↔ MySQL TINYINT
var (
	txStatusToInt = map[model.TxStatus]int{
		model.StatusTrying:     0,
		model.StatusConfirming: 1,
		model.StatusCancelling: 2,
		model.StatusCompleted:  3,
		model.StatusFailed:     4,
	}
	intToTxStatus = map[int]model.TxStatus{
		0: model.StatusTrying,
		1: model.StatusConfirming,
		2: model.StatusCancelling,
		3: model.StatusCompleted,
		4: model.StatusFailed,
	}
	branchStatusToInt = map[model.BranchStatus]int{
		model.BranchInit:        0,
		model.BranchTryDone:     1,
		model.BranchConfirmDone: 2,
		model.BranchCancelDone:  3,
	}
	intToBranchStatus = map[int]model.BranchStatus{
		0: model.BranchInit,
		1: model.BranchTryDone,
		2: model.BranchConfirmDone,
		3: model.BranchCancelDone,
	}
)

// MySQLRepository 是 Repository 接口的 MySQL 实现。
// 使用 database/sql 连接池，所有公开方法均接受 context 以支持超时和取消。
type MySQLRepository struct {
	DB *sql.DB
}

// NewMySQLRepository 创建 MySQL 仓储实例并验证数据库连通性。
//
//	dsn: MySQL 连接串，如 "user:pass@tcp(localhost:3306)/tcc?parseTime=true"
//
// 意义：构造函数负责建立连接池（默认 10 空闲 / 50 开放），
// 并用 Ping 确保启动时即可用，fail-fast 优于延迟报错。
func NewMySQLRepository(dsn string) (*MySQLRepository, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*myTime.MyTime)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("db.Ping: %w", err)
	}
	return &MySQLRepository{DB: db}, nil
}

// participantJSON 是 content 字段的内部序列化格式。
type participantJSON struct {
	ServiceName  string `json:"service_name"`
	ResourceData string `json:"resource_data"`
	Address      string `json:"address"`
}

// CreateTransaction 原子写入全局事务及其所有分支事务。
//
// 意义：使用数据库事务包裹多表写入，保证全局事务与分支事务的原子性——
// 要么全部写入，要么全部回滚，杜绝"有全局无分支"的半写入状态。
// 每个分支 INSERT 后读取自增 ID 回填到 Go 模型，后续 UpdateBranchStatus 依赖此 ID。
func (r *MySQLRepository) CreateTransaction(ctx context.Context, tx *model.Transaction) error {
	// 序列化参与者信息到 content 字段
	participants := make([]participantJSON, len(tx.Branches))
	for i, br := range tx.Branches {
		participants[i] = participantJSON{

			ServiceName:  br.ServiceName,
			ResourceData: br.ResourceData,
			Address:      br.Address,
		}
	}
	content, err := json.Marshal(participants)
	if err != nil {
		return fmt.Errorf("marshal participants: %w", err)
	}

	dbTx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BeginTx: %w", err)
	}
	defer dbTx.Rollback()

	svcName := ""
	if len(tx.Branches) > 0 {
		svcName = tx.Branches[0].ServiceName
	}
	_, err = dbTx.ExecContext(ctx,
		`INSERT INTO global_transaction (xid, status, service_name, create_time, update_time, timeout, content)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		tx.XID, txStatusToInt[tx.Status], svcName,
		tx.CreateTime, tx.UpdateTime, tx.Timeout, string(content),
	)
	if err != nil {
		return fmt.Errorf("insert global: %w", err)
	}

	// 逐分支插入并回填自增 branch_id
	for _, br := range tx.Branches {
		result, err := dbTx.ExecContext(ctx,
			`INSERT INTO branch_transaction (xid, resource_id, status, try_data, create_time, update_time)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			tx.XID, br.ResourceData, branchStatusToInt[br.Status],
			br.TryData, tx.CreateTime, tx.UpdateTime,
		)
		if err != nil {
			return fmt.Errorf("insert branch %s: %w", br.ServiceName, err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			return fmt.Errorf("LastInsertId: %w", err)
		}
		br.BranchID = id
	}

	return dbTx.Commit()
}

// GetTransaction 按 XID 查询完整事务（含所有分支）。
//
// 意义：先查 global_transaction 获取全局信息，再查 branch_transaction 获取分支列表，
// 组装为完整的 Transaction 对象返回。两次查询在同一方法内，调用方获得的就是完整快照。
func (r *MySQLRepository) GetTransaction(ctx context.Context, xid string) (*model.Transaction, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT xid, status, service_name, create_time, update_time, timeout, retry_count
		 FROM global_transaction WHERE xid = ?`, xid)

	var (
		statusInt  int
		svcName    string
		createTime time.Time
		updateTime time.Time
		timeout    int
		retryCount int
	)
	if err := row.Scan(&xid, &statusInt, &svcName, &createTime, &updateTime, &timeout, &retryCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get global: %w", err)
	}

	// 查询所有分支
	branchRows, err := r.DB.QueryContext(ctx,
		`SELECT branch_id, xid, resource_id, status, try_data, create_time, update_time
		 FROM branch_transaction WHERE xid = ? ORDER BY branch_id`, xid)
	if err != nil {
		return nil, fmt.Errorf("query branches: %w", err)
	}
	defer branchRows.Close()

	var branches []*model.BranchTransaction
	for branchRows.Next() {
		var (
			br           model.BranchTransaction
			brStatusInt  int
			brCreateTime time.Time
			brUpdateTime time.Time
			tryData      sql.NullString
			resourceID   sql.NullString
		)
		if err := branchRows.Scan(&br.BranchID, &xid, &resourceID, &brStatusInt,
			&tryData, &brCreateTime, &brUpdateTime); err != nil {
			return nil, fmt.Errorf("scan branch: %w", err)
		}
		br.Status = intToBranchStatus[brStatusInt]
		if tryData.Valid {
			br.TryData = tryData.String
		}
		if resourceID.Valid {
			br.ResourceData = resourceID.String
		}
		branches = append(branches, &br)
	}
	if err := branchRows.Err(); err != nil {
		return nil, fmt.Errorf("branchRows iterate: %w", err)
	}

	// 反序列化 content 字段恢复 ServiceName 和 Address
	var participants []participantJSON
	if err := r.loadContent(ctx, xid, &participants); err == nil {
		for i, br := range branches {
			if i < len(participants) {
				br.ServiceName = participants[i].ServiceName
				br.Address = participants[i].Address
			}
		}
	}

	return &model.Transaction{
		XID:        xid,
		Status:     intToTxStatus[statusInt],
		Branches:   branches,
		Timeout:    timeout,
		RetryCount: retryCount,
		CreateTime: createTime,
		UpdateTime: updateTime,
	}, nil
}

// loadContent 读取 global_transaction.content 并反序列化。
func (r *MySQLRepository) loadContent(ctx context.Context, xid string, v *[]participantJSON) error {
	var content sql.NullString
	err := r.DB.QueryRowContext(ctx,
		`SELECT content FROM global_transaction WHERE xid = ?`, xid).Scan(&content)
	if err != nil || !content.Valid || content.String == "" {
		return err
	}
	return json.Unmarshal([]byte(content.String), v)
}

// UpdateTransactionStatus 只更新全局事务的状态和更新时间。
//
// 意义：单字段更新，避免全量更新带来的锁竞争和网络开销。
// 状态更新是最频繁的写操作（Try→Confirming→Completed 等），需要轻量高效。
func (r *MySQLRepository) UpdateTransactionStatus(ctx context.Context, xid string, status model.TxStatus) error {
	_, err := r.DB.ExecContext(ctx,
		`UPDATE global_transaction SET status = ?, update_time = ? WHERE xid = ?`,
		txStatusToInt[status], time.Now(), xid)
	if err != nil {
		return fmt.Errorf("update tx status: %w", err)
	}
	return nil
}

// UpdateBranchStatus 只更新单个分支事务的状态和更新时间。
//
// 意义：分支状态在 Try→TryDone、TryDone→ConfirmDone/CancelDone 时更新，
// 用 branch_id（全局自增唯一）精确定位，避免 xid + service_name 组合键的歧义。
func (r *MySQLRepository) UpdateBranchStatus(ctx context.Context, branchID int64, status model.BranchStatus) error {
	_, err := r.DB.ExecContext(ctx,
		`UPDATE branch_transaction SET status = ?, update_time = ? WHERE branch_id = ?`,
		branchStatusToInt[status], time.Now(), branchID)
	if err != nil {
		return fmt.Errorf("update branch status: %w", err)
	}
	return nil
}

// FindTimeOutTransactions 查找所有需要恢复的全局事务。
//
// 意义：查询处于非终态（Trying/Confirming/Cancelling）且
// 距上次更新时间已超过事务超时阈值的全局事务。
// 这是恢复器扫描的入口——协调器可能已经宕机或这些事务被遗忘，
// 恢复器需要介入决策（继续 Confirm 还是执行 Cancel）。
//
//	WHERE status IN (0,1,2)                     — 非终态
//	  AND TIMESTAMPDIFF(SECOND, update_time, NOW()) > timeout  — 已超时
func (r *MySQLRepository) FindTimeOutTransactions(ctx context.Context) ([]*model.Transaction, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT xid, status, service_name, create_time, update_time, timeout, retry_count
		 FROM global_transaction
		 WHERE status IN (0, 1, 2)
		   AND TIMESTAMPDIFF(SECOND, update_time, NOW()) > timeout
		 ORDER BY update_time ASC`)
	if err != nil {
		return nil, fmt.Errorf("FindTimeOut: %w", err)
	}
	defer rows.Close()

	var txs []*model.Transaction
	for rows.Next() {
		var (
			xid                    string
			statusInt              int
			svcName                string
			createTime, updateTime time.Time
			timeout                int
			retryCount             int
		)
		if err := rows.Scan(&xid, &statusInt, &svcName, &createTime, &updateTime, &timeout, &retryCount); err != nil {
			return nil, fmt.Errorf("scan timeout tx: %w", err)
		}
		tx := &model.Transaction{
			XID:        xid,
			Status:     intToTxStatus[statusInt],
			Timeout:    timeout,
			RetryCount: retryCount,
			CreateTime: createTime,
			UpdateTime: updateTime,
		}
		txs = append(txs, tx)
	}
	return txs, rows.Err()
}

// ListTransactions 返回所有全局事务的摘要列表（不含分支详情）。
//
// 意义：前端列表页和 GinHandler.ListTransactions 使用此方法。
// 只返回全局事务的摘要字段，分支详情由 GetTransaction 按需查询。
func (r *MySQLRepository) ListTransactions(ctx context.Context) ([]*model.Transaction, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT xid, status, service_name, create_time, update_time, timeout, retry_count
		 FROM global_transaction
		 ORDER BY create_time DESC LIMIT 500`)
	if err != nil {
		return nil, fmt.Errorf("ListTransactions: %w", err)
	}
	defer rows.Close()

	var txs []*model.Transaction
	for rows.Next() {
		var (
			xid                    string
			statusInt              int
			svcName                string
			createTime, updateTime time.Time
			timeout                int
			retryCount             int
		)
		if err := rows.Scan(&xid, &statusInt, &svcName, &createTime, &updateTime, &timeout, &retryCount); err != nil {
			return nil, fmt.Errorf("scan list: %w", err)
		}
		tx := &model.Transaction{
			XID:        xid,
			Status:     intToTxStatus[statusInt],
			Timeout:    timeout,
			RetryCount: retryCount,
			CreateTime: createTime,
			UpdateTime: updateTime,
		}
		// 加载参与者信息以获取分支数量和服务名
		var participants []participantJSON
		var content sql.NullString
		if err := r.DB.QueryRowContext(ctx,
			`SELECT content FROM global_transaction WHERE xid = ?`, xid).Scan(&content); err == nil && content.Valid {
			if json.Unmarshal([]byte(content.String), &participants) == nil {
				for _, p := range participants {
					tx.Branches = append(tx.Branches, &model.BranchTransaction{
						ServiceName: p.ServiceName,
						Address:     p.Address,
					})
				}
			}
		}
		txs = append(txs, tx)
	}
	return txs, rows.Err()
}

// Close 关闭数据库连接池。
func (r *MySQLRepository) Close() error {
	return r.DB.Close()
}

// buildPlaceholders 生成 (?, ?, ?) 形式的占位符串，用于批量 INSERT。
func buildPlaceholders(n, cols int) string {
	parts := make([]string, n)
	row := "(" + strings.Repeat("?,", cols-1) + "?)"
	for i := range parts {
		parts[i] = row
	}
	return strings.Join(parts, ", ")
}

func (r *MySQLRepository) ClearAllTransactions(ctx context.Context) error {
	_, err := r.DB.ExecContext(ctx, "DELETE FROM global_transaction where true")
	if err != nil {
		return fmt.Errorf("clear all transactions: %w", err)
	}
	_, err = r.DB.ExecContext(ctx, "DELETE FROM branch_transaction WHERE true")
	if err != nil {
		return fmt.Errorf("clear all branch transactions: %w", err)
	}
	return nil
}

func (r *MySQLRepository) AddRetryCount(ctx context.Context, id string) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE global_transaction SET retry_count = retry_count + 1 WHERE xid=?", id)
	if err != nil {
		return fmt.Errorf("add retry count: %w", err)
	}
	return nil
}

func (r *MySQLRepository) GetBranchTransaction(ctx context.Context, id int64) (model.BranchStatus, error) {
	rows, err := r.DB.QueryContext(ctx, "SELECT status FROM branch_transaction WHERE branch_id=?", id)
	if err != nil {
		return "", fmt.Errorf("GetBranchTransaction: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status int
		if err := rows.Scan(&status); err != nil {
			return "", fmt.Errorf("GetBranchTransaction: %w", err)
		}
		return intToBranchStatus[status], nil
	}
	return "", nil
}

func (r *MySQLRepository) UpdateBranchTransaction(ctx context.Context, id int64, status model.BranchStatus) error {
	rows, err := r.DB.ExecContext(ctx, "UPDATE branch_transaction SET status=? WHERE branch_id=?", branchStatusToInt[status], id)
	if err != nil {
		return fmt.Errorf("UpdateBranchTransaction: %w", err)
	}
	count, err := rows.RowsAffected()
	if err != nil {
		return fmt.Errorf("UpdateBranchTransaction: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("UpdateError: branch transaction not found")
	}
	return nil

}
