package repository

import (
	"context"
	"database/sql"
	"fmt"
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

// Close 关闭数据库连接池。
func (r *MySQLRepository) Close() error {
	return r.DB.Close()
}
