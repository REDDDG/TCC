// 库存服务入口：启动 BranchService gRPC server，监听 :9092。
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
	lis, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterBranchServiceServer(s, branch.NewServer("InventoryService"))

	log.Println("Inventory service listening on :9092")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
