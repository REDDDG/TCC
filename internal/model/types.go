// Package model 定义 TCC 分布式事务的共享类型，包括全局事务状态、
// 分支事务状态、参与者信息等核心数据结构。
package model

import "time"

// TxStatus 表示全局事务的生命周期状态。
type TxStatus string

const (
	StatusTrying     TxStatus = "Trying"     // Try 阶段已完成，等待 Commit/Cancel 指令
	StatusConfirming TxStatus = "Confirming" // 正在执行 Confirm，尚未全部完成
	StatusCancelling TxStatus = "Cancelling" // 正在执行 Cancel 回滚，尚未全部完成
	StatusCompleted  TxStatus = "Completed"  // 全部 Confirm 成功，事务最终提交
	StatusFailed     TxStatus = "Failed"     // 全部 Cancel 完成，事务已回滚
)

// BranchStatus 表示单个分支事务在 TCC 三阶段中的进度。
type BranchStatus string

const (
	BranchInit        BranchStatus = "Init"        // 初始状态，尚未 Try
	BranchTryDone     BranchStatus = "TryDone"     // Try 预留资源成功
	BranchConfirmDone BranchStatus = "ConfirmDone" // Confirm 确认提交成功
	BranchCancelDone  BranchStatus = "CancelDone"  // Cancel 补偿回滚成功
)

// Participant 描述一次全局事务中的参与方，对应 proto 中的 Participant 消息。
type Participant struct {
	ServiceName  string `json:"service_name"`  // 服务名称，如 OrderService
	ResourceData string `json:"resource_data"` // 需要预留的资源描述，业务自定义
	Address      string `json:"address"`       // 分支服务 gRPC 地址，如 localhost:9091
}

// BranchTransaction 记录单个分支事务的运行时状态。
type BranchTransaction struct {
	BranchID     int64        `json:"branch_id"`          // 分支编号，从 1 开始自增
	ServiceName  string       `json:"service_name"`       // 服务名称
	Address      string       `json:"address"`            // 分支 gRPC 地址
	ResourceData string       `json:"resource_data"`      // 预留资源描述
	Status       BranchStatus `json:"status"`             // 当前分支状态
	TryData      string       `json:"try_data,omitempty"` // Try 阶段返回的业务数据（预留）
}

// Transaction 表示一次完整的全局 TCC 事务。
type Transaction struct {
	XID        string               `json:"xid"`         // 全局事务唯一标识（UUID）
	Status     TxStatus             `json:"status"`      // 当前全局事务状态
	Branches   []*BranchTransaction `json:"branches"`    // 所有分支事务列表
	Timeout    int                  `json:"timeout"`     // 超时时间（秒），默认 30
	RetryCount int                  `json:"retry_count"` // 恢复器重试次数
	CreateTime time.Time            `json:"create_time"` // 事务创建时间
	UpdateTime time.Time            `json:"update_time"` // 最后一次状态变更时间
}

// NewTransaction 创建一个新的全局事务，初始状态为 StatusTrying。
//   - xid: 全局事务唯一标识，由调用方生成（UUID）
//   - branches: 预初始化的分支事务列表，每个分支状态为 BranchInit
//   - timeout: 超时秒数，若为 0 则默认 30 秒
func NewTransaction(xid string, branches []*BranchTransaction, timeout int) *Transaction {
	if timeout <= 0 {
		timeout = 30
	}
	now := time.Now()
	return &Transaction{
		XID:        xid,
		Status:     StatusTrying,
		Branches:   branches,
		Timeout:    timeout,
		CreateTime: now,
		UpdateTime: now,
	}
}
