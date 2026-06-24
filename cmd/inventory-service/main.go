// 库存服务入口：启动 BranchService gRPC server，监听 :9092。
// 模拟 TCC 分支服务，提供 Try/Confirm/Cancel 三阶段资源操作。
package main

import (
	"log"
	"net"
	"os"
	"tcc/internal/branch/inventory"
	"tcc/internal/repository"

	pb "tcc/api/proto/branch"

	"google.golang.org/grpc"
)

// main 注册 BranchServiceServer 并启动 gRPC server，阻塞直到进程退出。
func main() {
	lis, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		//仅在开发学习中直接写入密码
		dsn = "root:mysql12138@tcp(127.0.0.1:3306)/tcc?parseTime=true"
	}

	var repo repository.Repository
	mysqlRepo, err := repository.NewMySQLRepository(dsn)
	if err != nil {
		log.Printf("[main] MySQL not available (%v), falling back to in-memory mode", err)
	} else {
		repo = mysqlRepo
		log.Println("[main] MySQL connected, tables ensured")
	}
	s := grpc.NewServer()
	pb.RegisterBranchServiceServer(s, inventory.NewServer("InventoryService", repo))

	log.Println("Inventory service listening on :9092")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
