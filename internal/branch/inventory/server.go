// Package inventory 提供 TCC 分支服务的模拟实现，用于本地开发和测试。
// 三个服务 (Order/Inventory/Points) 共用此实现，通过 ServiceName 区分。
package inventory

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	pb "tcc/api/proto/branch"
	"tcc/internal/model"
	"tcc/internal/repository"
)

// Server 实现 BranchServiceServer 接口，模拟一个 TCC 分支服务。
type Server struct {
	pb.UnimplementedBranchServiceServer
	ServiceName string
	mu          sync.Mutex
	repo        repository.Repository
}

// NewServer 创建一个分支服务实例。
//   - name: 服务名称，用于日志和错误消息中标识自身
func NewServer(name string, repo repository.Repository) *Server {
	return &Server{
		ServiceName: name,
		repo:        repo,
	}
}

// Try 实现 TCC 的 Try 阶段：尝试预留资源。
//   - ctx: gRPC 调用上下文
//   - req.Xid: 全局事务 ID
//   - req.ResourceData: 需要预留的资源描述
//   - 返回: TryResponse.Success 为 true 表示预留成功；false 表示资源不足或已被取消
func (s *Server) Try(ctx context.Context, req *pb.TryRequest) (*pb.TryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 空回滚检测：Cancel 先于 Try 到达，直接拒绝防止悬挂资源
	st, err := s.repo.GetBranchTransaction(ctx, req.Xid)
	if err != nil {
		return &pb.TryResponse{Success: false, Error: "StatusError"}, err
	}
	if st == model.BranchCancelDone {
		return &pb.TryResponse{Success: false, Error: "StatusCancel"}, nil
	}
	err = s.repo.InventoryTry(ctx, req.Xid)
	if err != nil {
		return &pb.TryResponse{Success: false, Error: "TryError"}, err
	}
	// 90% 成功率，模拟生产环境中资源不足的场景
	if rand.Intn(10) < 1 {
		return &pb.TryResponse{Success: false, Error: fmt.Sprintf("%s: resource occupied", s.ServiceName)}, nil
	}

	err = s.repo.UpdateBranchTransaction(ctx, req.Xid, model.BranchTryDone)
	if err != nil {
		return nil, err
	}
	return &pb.TryResponse{Success: true}, nil
}

// Confirm 实现 TCC 的 Confirm 阶段：确认提交已预留的资源。
//   - ctx: gRPC 调用上下文
//   - req.Xid: 全局事务 ID
//   - 返回: ConfirmResponse.Success 为 true 表示确认完成（Phase 1 中始终成功）
func (s *Server) Confirm(ctx context.Context, req *pb.ConfirmRequest) (*pb.ConfirmResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 模拟处理延迟 50-150ms
	//time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)

	err := s.repo.UpdateBranchTransaction(ctx, req.Xid, model.BranchConfirmDone)
	if err != nil {
		return nil, err
	}
	//return &pb.ConfirmResponse{Success: false}, nil,测试重试机制用
	return &pb.ConfirmResponse{Success: true}, nil
}

// Cancel 实现 TCC 的 Cancel 阶段：回滚已预留的资源。
//   - ctx: gRPC 调用上下文
//   - req.Xid: 全局事务 ID
//   - 返回: CancelResponse.Success 为 true 表示回滚完成（Phase 1 中始终成功）
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 模拟处理延迟 50-150ms
	//time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
	err := s.repo.InventoryCancel(ctx, req.Xid)
	if err != nil {
		return &pb.CancelResponse{Success: false}, err
	}
	err = s.repo.UpdateBranchTransaction(ctx, req.Xid, model.BranchCancelDone)
	if err != nil {
		return nil, err
	}
	return &pb.CancelResponse{Success: true}, nil
}
