// 协调器入口：启动 gRPC server (:9090) 和 Gin HTTP server (:8080)。
// gRPC 负责 TCC 事务协议编排；Gin 提供前端页面和 REST API。
package main

import (
	"log"
	"net"
	coordpb "tcc/api/proto/coordinator"
	"tcc/internal/coordinator"
	"tcc/internal/middleware"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

// main 创建共享 Store，启动 gRPC 和 HTTP 两个 server。
// gRPC 在 goroutine 中运行，HTTP 阻塞主线程。
func main() {
	store := coordinator.NewStore()

	// gRPC Server：监听 :9090，注册 TCCCoordinatorServer
	grpcServer := grpc.NewServer()
	coordpb.RegisterTCCCoordinatorServer(grpcServer, coordinator.NewGRPCServer(store))

	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go func() {
		log.Println("gRPC coordinator listening on :9090")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Gin HTTP Server：监听 :8080，提供前端页面和 REST API
	ginHandler := coordinator.NewGinHandler("127.0.0.1:9090", store)

	r := gin.New()
	r.Use(gin.Recovery())
	r.StaticFile("/", "./web/index.html")

	api := r.Group("/api/v1")
	{
		api.Use(middleware.LoggerSkip(api.BasePath() + "/health"))
		api.POST("/transactions", ginHandler.CreateTransaction)
		api.GET("/transactions", ginHandler.ListTransactions)
		api.GET("/transactions/:xid", ginHandler.GetTransaction)
		api.POST("/transactions/:xid/commit", ginHandler.CommitTransaction)
		api.GET("/health", ginHandler.HealthCheck)
	}

	log.Println("Gin HTTP listening on :8080")

_:
	r.Run(":8080")
}
