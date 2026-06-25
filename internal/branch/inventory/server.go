// Package inventory 提供 TCC 分支服务的模拟实现，用于本地开发和测试。
// 三个服务 (Order/Inventory/Points) 共用此实现，通过 ServiceName 区分。
package inventory

import (
	"context"
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
func (s *Server) Try(ctx context.Context, req *pb.TryRequest) (*pb.TryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.repo.GetBranchTransaction(ctx, req.BranchId)
	if err != nil {
		return &pb.TryResponse{Success: false, Error: "StatusError"}, err
	}
	if st == model.BranchCancelDone {
		return &pb.TryResponse{Success: false, Error: "StatusCancel"}, nil
	}

	if err := s.repo.InventoryTry(ctx, req.ResourceData); err != nil {
		return &pb.TryResponse{Success: false, Error: "TryError"}, err
	}

	//if rand.Intn(10) < 1 {
	//	return &pb.TryResponse{Success: false, Error: fmt.Sprintf("%s: resource occupied", s.ServiceName)}, nil
	//}

	if err := s.repo.UpdateBranchTransaction(ctx, req.BranchId, model.BranchTryDone); err != nil {
		return nil, err
	}
	return &pb.TryResponse{Success: true}, nil
}

// Confirm 实现 TCC 的 Confirm 阶段：确认提交已预留的资源。
func (s *Server) Confirm(ctx context.Context, req *pb.ConfirmRequest) (*pb.ConfirmResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.repo.InventoryConfirm(ctx, req.ResourceData); err != nil {
		return &pb.ConfirmResponse{Success: false, Error: "ConfirmError"}, err
	}

	if err := s.repo.UpdateBranchTransaction(ctx, req.BranchId, model.BranchConfirmDone); err != nil {
		return nil, err
	}
	return &pb.ConfirmResponse{Success: true}, nil
}

// Cancel 实现 TCC 的 Cancel 阶段：回滚已预留的资源。
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.repo.InventoryCancel(ctx, req.ResourceData); err != nil {
		return &pb.CancelResponse{Success: false}, err
	}
	if err := s.repo.UpdateBranchTransaction(ctx, req.BranchId, model.BranchCancelDone); err != nil {
		return nil, err
	}
	return &pb.CancelResponse{Success: true}, nil
}
