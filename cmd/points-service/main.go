// 积分服务入口：启动 BranchService gRPC server，监听 :9093。
// 模拟 TCC 分支服务，提供 Try/Confirm/Cancel 三阶段资源操作。
package main

import (
	"log"
	"net"

	pb "tcc/api/proto/branch"
	"tcc/internal/branch"

	"google.golang.org/grpc"
)

// main 注册 BranchServiceServer 并启动 gRPC server，阻塞直到进程退出。
func main() {
	lis, err := net.Listen("tcp", ":9093")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterBranchServiceServer(s, branch.NewServer("PointsService"))

	log.Println("Points service listening on :9093")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
