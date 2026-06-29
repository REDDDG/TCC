package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"tcc/internal/myTime"
	"time"

	branchpb "tcc/api/proto/branch"
	coordpb "tcc/api/proto/coordinator"
	"tcc/internal/model"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCServer 实现 TCCCoordinatorServer 接口，是 TCC 事务协议的核心编排引擎。
// 负责生成 XID、管理状态机、调度 Try/Confirm/Cancel 三阶段流程。
type GRPCServer struct {
	coordpb.UnimplementedTCCCoordinatorServer
	store     *Store
	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn
	brClients map[string]branchpb.BranchServiceClient
}

// NewGRPCServer 创建 gRPC 协调器服务实例。
//   - store: 事务内存存储，由 cmd/coordinator 注入
func NewGRPCServer(store *Store) *GRPCServer {
	return &GRPCServer{store: store, conns: make(map[string]*grpc.ClientConn),
		brClients: make(map[string]branchpb.BranchServiceClient)}
}

// Begin 开启一个全局事务：生成 XID → 初始化分支 → 顺序调 Try → 全成功则等待，任一失败则回滚。
//   - ctx: gRPC 调用上下文
//   - req.Participants: 参与方列表，每个含服务名、资源数据、gRPC 地址
//   - req.Timeout: 预留字段，Phase 1 未使用
//   - 返回: BeginResponse.Xid 为全局事务 ID；Success 为 true 表示全部 Try 通过；false 时已自动回滚
func (s *GRPCServer) Begin(ctx context.Context, req *coordpb.BeginRequest) (*coordpb.BeginResponse, error) {
	xid := uuid.New().String()
	log.Printf("[coordinator] Begin XID=%s, participants=%d", xid, len(req.Participants))

	branches := make([]*model.BranchTransaction, len(req.Participants))
	for i, p := range req.Participants {
		branches[i] = &model.BranchTransaction{
			BranchID:     int64(i + 1),
			ServiceName:  p.ServiceName,
			Address:      p.Address,
			ResourceData: p.ResourceData,
			Status:       model.BranchInit,
		}
	}

	tx := model.NewTransaction(xid, branches, int(req.Timeout))
	s.store.Create(tx)

	results := make([]*coordpb.BranchResult, len(req.Participants))
	allOK := true

	for i, p := range req.Participants {
		err := s.callBranchTry(branches[i], xid, p.ResourceData, p.Address)
		if err != nil {
			results[i] = &coordpb.BranchResult{ServiceName: p.ServiceName, Success: false, Error: err.Error()}
			allOK = false
			branches[i].Status = model.BranchInit
			break
		} else {
			results[i] = &coordpb.BranchResult{ServiceName: p.ServiceName, Success: true}
			branches[i].Status = model.BranchTryDone
		}
	}

	if !allOK {
		_, err := s.Cancel(ctx, &coordpb.CancelRequest{Xid: xid})
		if err != nil {
			return nil, err
		}
	} else {
		_, err := s.Commit(ctx, &coordpb.CommitRequest{Xid: xid})
		if err != nil {
			return nil, err
		}
	}
	tx.UpdateTime = time.Now()
	s.store.Update(tx)

	return &coordpb.BeginResponse{
		Xid:           xid,
		Success:       allOK,
		Error:         boolToError(allOK),
		BranchResults: results,
	}, nil
}

// Commit 确认提交全局事务：检查状态 → 对每个 TryDone 分支调 Confirm → 全成功则标记 Completed。
//   - ctx: gRPC 调用上下文
//   - req.Xid: 需要提交的全局事务 ID
//   - 返回: CommitResponse.Success 为 true 表示全部 Confirm 成功；false 时已记录错误，事务停留在 Confirming 状态
func (s *GRPCServer) Commit(ctx context.Context, req *coordpb.CommitRequest) (*coordpb.CommitResponse, error) {
	log.Printf("[coordinator] Commit XID=%s", req.Xid)

	tx, ok := s.store.Get(req.Xid)
	if !ok {
		return &coordpb.CommitResponse{Success: false, Error: "transaction not found"}, nil
	}

	if tx.Status != model.StatusTrying {
		return &coordpb.CommitResponse{Success: false, Error: fmt.Sprintf("invalid status: %s", tx.Status)}, nil
	}

	tx.Status = model.StatusConfirming
	s.store.Update(tx)

	for _, br := range tx.Branches {
		if br.Status == model.BranchTryDone {
			if err := s.callBranchConfirm(br, tx.XID, br.Address); err != nil {
				log.Printf("[coordinator] Confirm branch %s failed: %v", br.ServiceName, err)
				tx.UpdateTime = time.Now()
				s.store.Update(tx)
				return &coordpb.CommitResponse{Success: false, Error: fmt.Sprintf("confirm %s failed: %v", br.ServiceName, err)}, nil
			}
			br.Status = model.BranchConfirmDone
		}
	}

	tx.Status = model.StatusCompleted
	tx.UpdateTime = time.Now()
	s.store.Update(tx)

	log.Printf("[coordinator] Commit XID=%s Completed", req.Xid)
	return &coordpb.CommitResponse{Success: true}, nil
}

// Cancel 补偿回滚全局事务：对所有 TryDone 分支调 Cancel → 全部处理后标记 Failed。
// 与 Commit 不同，单个分支 Cancel 失败不会中断——继续处理剩余分支，仅记录日志。
//   - ctx: gRPC 调用上下文
//   - req.Xid: 需要回滚的全局事务 ID
//   - 返回: CancelResponse.Success 为 true 表示 Cancel 流程执行完毕（即使个别分支失败）
func (s *GRPCServer) Cancel(ctx context.Context, req *coordpb.CancelRequest) (*coordpb.CancelResponse, error) {
	log.Printf("[coordinator] Cancel XID=%s", req.Xid)

	tx, ok := s.store.Get(req.Xid)
	if !ok {
		return &coordpb.CancelResponse{Success: false, Error: "transaction not found"}, nil
	}

	tx.Status = model.StatusCancelling
	s.store.Update(tx)

	for _, br := range tx.Branches {
		if br.Status == model.BranchTryDone {
			if err := s.callBranchCancel(br, tx.XID, br.Address); err != nil {
				log.Printf("[coordinator] Cancel branch %s failed: %v", br.ServiceName, err)
			} else {
				br.Status = model.BranchCancelDone
			}
		}
	}

	tx.Status = model.StatusFailed
	tx.UpdateTime = time.Now()
	s.store.Update(tx)

	log.Printf("[coordinator] Cancel XID=%s Failed", req.Xid)
	return &coordpb.CancelResponse{Success: true}, nil
}

// GetStatus 查询全局事务及所有分支的当前状态。
//   - ctx: gRPC 调用上下文
//   - req.Xid: 全局事务 ID
//   - 返回: StatusResponse 包含全局状态和每个分支的服务名 + 状态
func (s *GRPCServer) GetStatus(ctx context.Context, req *coordpb.StatusRequest) (*coordpb.StatusResponse, error) {
	tx, ok := s.store.Get(req.Xid)
	if !ok {
		return &coordpb.StatusResponse{Xid: req.Xid, Status: "NOT_FOUND"}, nil
	}

	branchStatuses := make([]*coordpb.BranchStatus, len(tx.Branches))
	for i, br := range tx.Branches {
		branchStatuses[i] = &coordpb.BranchStatus{
			ServiceName: br.ServiceName,
			Status:      string(br.Status),
		}
	}

	return &coordpb.StatusResponse{
		Xid:            tx.XID,
		Status:         string(tx.Status),
		BranchStatuses: branchStatuses,
	}, nil
}

// --- 分支服务 gRPC 调用辅助函数 ---

// callBranchTry 对指定分支服务发起 Try RPC 远程调用。
//   - br: 分支事务对象（用于日志）
//   - xid: 全局事务 ID
//   - resourceData: 需要预留的资源描述
//   - addr: 分支 gRPC 地址，如 "localhost:9091"
//   - 返回: nil 表示 Try 预留成功；非 nil 表示网络错误或业务拒绝
func (s *GRPCServer) callBranchTry(br *model.BranchTransaction, xid, resourceData, addr string) error {
	client, err := s.getBranchClient(addr)
	if err != nil {
		return fmt.Errorf("getBranchClient %s: %w", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*myTime.MyTime)
	defer cancel()
	start := time.Now()
	resp, err := client.Try(ctx, &branchpb.TryRequest{BranchId: br.BranchID, ResourceData: resourceData, Xid: xid})
	fmt.Println("clientCallBranchTry:", time.Since(start))
	if err != nil {
		return fmt.Errorf("try rpc: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// callBranchConfirm 对指定分支服务发起 Confirm RPC 远程调用。
//   - br: 分支事务对象
//   - xid: 全局事务 ID
//   - addr: 分支 gRPC 地址
//   - 返回: nil 表示 Confirm 确认成功；非 nil 表示网络错误或业务拒绝
func (s *GRPCServer) callBranchConfirm(br *model.BranchTransaction, xid, addr string) error {
	client, err := s.getBranchClient(addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*myTime.MyTime)
	defer cancel()

	resp, err := client.Confirm(ctx, &branchpb.ConfirmRequest{BranchId: br.BranchID, ResourceData: br.ResourceData, Xid: xid})
	if err != nil {
		return fmt.Errorf("confirm rpc: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// callBranchCancel 对指定分支服务发起 Cancel RPC 远程调用。
//   - br: 分支事务对象
//   - xid: 全局事务 ID
//   - addr: 分支 gRPC 地址
//   - 返回: nil 表示 Cancel 回滚成功；非 nil 表示网络错误或业务拒绝
func (s *GRPCServer) callBranchCancel(br *model.BranchTransaction, xid, addr string) error {
	client, err := s.getBranchClient(addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*myTime.MyTime)
	defer cancel()

	resp, err := client.Cancel(ctx, &branchpb.CancelRequest{BranchId: br.BranchID, ResourceData: br.ResourceData, Xid: xid})
	if err != nil {
		return fmt.Errorf("cancel rpc: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// boolToError 将 Try 全部成功的布尔标志转为 BeginResponse 的 error 字符串。
//   - ok: true 表示全部成功，返回空字符串；false 表示存在失败分支
func boolToError(ok bool) string {
	if ok {
		return ""
	}
	return "one or more branches Try failed"
}

func (s *GRPCServer) getBranchClient(addr string) (branchpb.BranchServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cli, ok := s.brClients[addr]; ok {
		return cli, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("NewClient %s: %w", addr, err)
	}
	cli := branchpb.NewBranchServiceClient(conn)
	s.conns[addr] = conn
	s.brClients[addr] = cli
	return cli, nil
}
