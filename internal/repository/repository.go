package repository

import (
	"context"
	"tcc/internal/model"
)

// Repository 定义事务持久化层的抽象接口。
// Phase 1 为内存实现，Phase 2 起切换为 MySQL 实现。
// 接口仅暴露必要的持久化操作，不暴露底层实现细节。
type Repository interface {
	// CreateTransaction 创建全局事务及其所有分支事务（原子写入）。
	CreateTransaction(ctx context.Context, tx *model.Transaction) error

	// GetTransaction 按 XID 查询全局事务，包含所有分支事务。
	// 未找到时返回 nil, nil。
	GetTransaction(ctx context.Context, xid string) (*model.Transaction, error)

	// GetBranchTransaction 按id查询分支事务
	GetBranchTransaction(ctx context.Context, id int64) (model.BranchStatus, error)

	UpdateBranchTransaction(ctx context.Context, id int64, status model.BranchStatus) error

	// UpdateTransactionStatus 只更新全局事务的状态字段和更新时间。
	UpdateTransactionStatus(ctx context.Context, xid string, status model.TxStatus) error

	// UpdateBranchStatus 只更新单个分支事务的状态字段。
	UpdateBranchStatus(ctx context.Context, branchID int64, status model.BranchStatus) error

	// FindTimeOutTransactions 查找所有处于非终态且已超时的全局事务。
	// 用于恢复器扫描。
	FindTimeOutTransactions(ctx context.Context) ([]*model.Transaction, error)

	// ListTransactions 返回所有全局事务的摘要（不含分支详情，用于列表展示）。
	ListTransactions(ctx context.Context) ([]*model.Transaction, error)

	// ClearAllTransactions 删除所有事务
	ClearAllTransactions(ctx context.Context) error

	// AddRetryCount 重试计数+1
	AddRetryCount(ctx context.Context, id string) error

	// InventoryTry 尝试预留库存
	InventoryTry(ctx context.Context, productId string) error

	// InventoryConfirm 确定更新库存，修改更新日期
	InventoryConfirm(ctx context.Context, productId string) error

	// InventoryCancel 取消库存预留
	InventoryCancel(ctx context.Context, productId string) error

	// OrderTry 尝试创建订单
	OrderTry(ctx context.Context, order model.Order) error

	// OrderConfirm 更新订单修改日期
	OrderConfirm(ctx context.Context, order model.Order) error

	// OrderCancel 取消订单
	OrderCancel(ctx context.Context, order model.Order) error

	// PointsTry 尝试扣减积分
	PointsTry(ctx context.Context, account model.PointsAccount) error

	// PointsConfirm 更新积分修改日期
	PointsConfirm(ctx context.Context, account model.PointsAccount) error

	// PointsCancel 取消积分扣减
	PointsCancel(ctx context.Context, account model.PointsAccount) error
}
