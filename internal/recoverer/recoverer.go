package recoverer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	branchpb "tcc/api/proto/branch"
	"tcc/internal/model"
	"tcc/internal/repository"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Recoverer 定义超时事务恢复操作的接口。
//
// 意义：将恢复策略（"怎么恢复"）与扫描调度（"何时恢复"）解耦。
// Scanner 只负责发现超时事务，Recoverer 决定如何处理，符合单一职责原则。
type Recoverer interface {
	Cancel(ctx context.Context, tx *model.Transaction) error
	Recover(ctx context.Context, tx *model.Transaction) error
}

// DefaultRecoverer 实现 Recoverer 接口，对超时事务执行补偿恢复。
//
// 恢复策略（fail-safe 导向）：
//   - StatusTrying → Cancel：Begin 成功但无人 Commit，回滚所有预留资源
//   - StatusConfirming → Cancel：Commit 中途失败，与其重试不确定的 Confirm，不如全部回滚
//   - StatusCancelling → 重试 Cancel：Cancel 中途失败，继续 Cancel 未完成的分支
//
// 所有操作均为幂等——分支服务的 Cancel 允许多次调用。
type DefaultRecoverer struct {
	repo      repository.Repository
	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn             // addr → conn
	brClients map[string]branchpb.BranchServiceClient // addr → client
}

// NewDefaultRecoverer 创建恢复器实例。
//
//	repo: 仓储接口，用于加载完整事务和更新状态
func NewDefaultRecoverer(repo repository.Repository) *DefaultRecoverer {
	return &DefaultRecoverer{
		repo:      repo,
		conns:     make(map[string]*grpc.ClientConn),
		brClients: make(map[string]branchpb.BranchServiceClient),
	}
}

// Cancel 对单个超时事务执行恢复操作。
//
// 流程：
//  1. 从仓储加载完整事务（含分支详情、参与者地址）
//  2. 根据全局状态决策：所有非终态统一 Cancel
//  3. 对每个 TryDone 分支调用 Cancel RPC
//  4. 更新全局状态为 Failed
//
// 意义：这是"最终一致性"的兜底机制——即使协调器宕机或网络中断，
// 资源也不会永久锁定。所有分支的 Cancel 都是幂等的。
func (r *DefaultRecoverer) Cancel(ctx context.Context, tx *model.Transaction) error {
	// 加载完整事务（包含分支详情和参与者地址）
	fullTx, err := r.repo.GetTransaction(ctx, tx.XID)
	if err != nil {
		return fmt.Errorf("cancel: get full tx %s: %w", tx.XID, err)
	}
	if fullTx == nil {
		return fmt.Errorf("cancel: tx %s not found", tx.XID)
	}

	log.Printf("[recoverer] canceling XID=%s status=%s branches=%d",
		fullTx.XID, fullTx.Status, len(fullTx.Branches))

	// 对所有 TryDone 分支执行 Cancel（幂等安全）
	var lastErr error
	for _, br := range fullTx.Branches {
		if br.Status != model.BranchTryDone && br.Status != model.BranchConfirmDone {
			continue // Init 或已 CancelDone 的分支无需处理
		}
		if br.Address == "" {
			log.Printf("[recoverer] branch %s (id=%d) has no address, skip", br.ServiceName, br.BranchID)
			continue
		}
		if err := r.callBranchCancel(ctx, br, fullTx.XID, br.Address); err != nil {
			log.Printf("[recoverer] cancel branch %s (id=%d) failed: %v",
				br.ServiceName, br.BranchID, err)
			lastErr = err
		} else {
			if err := r.repo.UpdateBranchStatus(ctx, br.BranchID, model.BranchCancelDone); err != nil {
				log.Printf("[recoverer] UpdateBranchStatus %d failed: %v", br.BranchID, err)
			}
		}
	}

	// 更新全局状态为 Failed（所有 Cancel 已完成）
	if err := r.repo.UpdateTransactionStatus(ctx, fullTx.XID, model.StatusFailed); err != nil {
		return fmt.Errorf("cancel: update tx status to Failed: %w", err)
	}

	if lastErr != nil {
		return fmt.Errorf("cancel %s partially failed: %w", fullTx.XID, lastErr)
	}
	log.Printf("[recoverer] XID=%s canceled → Failed", fullTx.XID)
	return nil
}

// Recover 对尚未确认完毕的事务进行重试
func (r *DefaultRecoverer) Recover(ctx context.Context, tx *model.Transaction) error {
	fullTx, err := r.repo.GetTransaction(ctx, tx.XID)
	if err != nil {
		return fmt.Errorf("recover: get full tx %s: %w", tx.XID, err)
	}
	if fullTx == nil {
		return fmt.Errorf("recover: tx %s not found", tx.XID)
	}
	var lastErr error
	for _, br := range fullTx.Branches {
		if br.Status != model.BranchConfirmDone {
			if err := r.callBranchConfirm(ctx, br, fullTx.XID, br.Address); err != nil {
				log.Printf("[recoverer] recover branch %s (id=%d) failed: %v", br.ServiceName, br.BranchID, err)
				lastErr = err
			} else {
				if err := r.repo.UpdateBranchStatus(ctx, br.BranchID, model.BranchConfirmDone); err != nil {
					log.Printf("[recoverer] UpdateBranchStatus %d failed: %v", br.BranchID, err)
				}
			}
		}
	}
	if err := r.repo.UpdateTransactionStatus(ctx, fullTx.XID, model.StatusCompleted); err != nil {
		return fmt.Errorf("recover: update tx status to Failed: %w", err)
	}
	if lastErr != nil {
		return fmt.Errorf("recover %s partially failed: %w", fullTx.XID, lastErr)
	}
	log.Printf("[recoverer] XID=%s recovered → Failed", fullTx.XID)
	return nil
}

// callBranchCancel 对指定分支服务发起 Cancel RPC。
//
// 3 秒超时足够覆盖正常 Cancel 的 50-150ms 模拟延迟。
// 与 GRPCServer.callBranchCancel 功能相同但独立实现，
// 避免耦合协调器的生命周期。
func (r *DefaultRecoverer) callBranchCancel(ctx context.Context, br *model.BranchTransaction, xid, addr string) error {
	client, err := r.getBranchClient(addr)
	if err != nil {
		return fmt.Errorf("getBranchClient %s: %w", addr, err)
	}

	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := client.Cancel(callCtx, &branchpb.CancelRequest{Xid: xid})
	if err != nil {
		return fmt.Errorf("cancel rpc: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// callBranchConfirm 对指定分支服务发起 Cancel RPC。
//
// 3 秒超时足够覆盖正常 Cancel 的 50-150ms 模拟延迟。
// 与 GRPCServer.callBranchCancel 功能相同但独立实现，
// 避免耦合协调器的生命周期。
func (r *DefaultRecoverer) callBranchConfirm(ctx context.Context, br *model.BranchTransaction, xid, addr string) error {
	client, err := r.getBranchClient(addr)
	if err != nil {
		return fmt.Errorf("getBranchClient %s: %w", addr, err)
	}

	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := client.Confirm(callCtx, &branchpb.ConfirmRequest{Xid: xid})
	if err != nil {
		return fmt.Errorf("ConfirmRequest rpc: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// getBranchClient 获取或创建到指定地址的 gRPC 客户端。
//
// 意义：连接复用——同一分支服务只需建立一次连接。
// 连接永远不会主动关闭（进程生命周期内），简化了生命周期管理。
func (r *DefaultRecoverer) getBranchClient(addr string) (branchpb.BranchServiceClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cli, ok := r.brClients[addr]; ok {
		return cli, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("NewClient %s: %w", addr, err)
	}
	cli := branchpb.NewBranchServiceClient(conn)
	r.conns[addr] = conn
	r.brClients[addr] = cli
	return cli, nil
}
