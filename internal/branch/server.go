// Package branch 提供 TCC 分支服务的模拟实现，用于本地开发和测试。
// 三个服务 (Order/Inventory/Points) 共用此实现，通过 ServiceName 区分。
package branch

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	pb "tcc/api/proto/branch"
)

// Server 实现 BranchServiceServer 接口，模拟一个 TCC 分支服务。
// Phase 1 为单节点 MVP：纯内存状态，随机延迟，90% Try 成功率。
type Server struct {
	pb.UnimplementedBranchServiceServer
	ServiceName string

	mu     sync.Mutex
	states map[string]string // xid -> 本地状态: "try_ok" | "confirmed" | "cancelled"
}

// NewServer 创建一个分支服务实例。
//   - name: 服务名称，用于日志和错误消息中标识自身
func NewServer(name string) *Server {
	return &Server{
		ServiceName: name,
		states:      make(map[string]string),
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
	if st, ok := s.states[req.Xid]; ok && st == "cancelled" {
		return &pb.TryResponse{Success: false, Error: "cancelled before try"}, nil
	}

	// 模拟处理延迟 100-500ms
	//time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond)

	// 90% 成功率，模拟生产环境中资源不足的场景
	if rand.Intn(10) < 1 {
		return &pb.TryResponse{Success: false, Error: fmt.Sprintf("%s: resource occupied", s.ServiceName)}, nil
	}

	s.states[req.Xid] = "try_ok"
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

	s.states[req.Xid] = "confirmed"
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

	s.states[req.Xid] = "cancelled"
	return &pb.CancelResponse{Success: true}, nil
}
