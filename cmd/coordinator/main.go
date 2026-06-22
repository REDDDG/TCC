// 协调器入口：启动 gRPC server (:9090) 和 Gin HTTP server (:8080)。
// gRPC 负责 TCC 事务协议编排；Gin 提供前端页面和 REST API。
//
// Phase 2: 集成 MySQL 持久化、超时扫描器与恢复器。
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	coordpb "tcc/api/proto/coordinator"
	"tcc/internal/coordinator"
	"tcc/internal/middleware"
	"tcc/internal/recoverer"
	"tcc/internal/repository"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

func main() {
	// ── MySQL 持久化层 ──
	// 通过 MYSQL_DSN 环境变量配置；留空则退化为纯内存模式（Phase 1 兼容）。
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

	// ── 共享存储层（MySQL 持久化 + 内存缓存）──
	store := coordinator.NewStore(repo)

	// ── gRPC Server：监听 :9090，注册 TCCCoordinatorServer ──
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

	// ── 超时恢复器 ──
	// Scanner 定时（默认 10s）扫描 MySQL 中已超时的非终态事务，
	// 交给 Recoverer 执行补偿 Cancel，避免资源永久悬挂。
	// 仅在 MySQL 可用时启动（纯内存模式无需恢复，重启即清空）。
	scannerCancel := func() {} // 默认空操作，MySQL 不可用时无 scanner 需停止
	if repo != nil {
		rec := recoverer.NewDefaultRecoverer(repo)
		scanner := recoverer.NewTimeoutScanner(repo, rec, 10*time.Second)

		var scannerCtx context.Context
		scannerCtx, scannerCancel = context.WithCancel(context.Background())

		go func() {
			if err := scanner.Run(scannerCtx); err != nil {
				log.Printf("[main] scanner exited: %v", err)
			}
		}()
	}

	// ── Gin HTTP Server：监听 :8080 ──
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
		api.GET("/health", ginHandler.HealthCheck)
		api.POST("/clear", ginHandler.ClearAllTransaction)
	}

	go func() {
		log.Println("Gin HTTP listening on :8080")
		if err := r.Run(":8080"); err != nil {
			log.Fatalf("Gin server failed: %v", err)
		}
	}()

	// ── 优雅关闭 ──
	// 捕获 SIGINT/SIGTERM，停止 scanner、关闭 gRPC 和 MySQL 连接池。
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[main] shutting down...")
	scannerCancel()           // 停止扫描循环
	grpcServer.GracefulStop() // 等待进行中的 RPC 完成
	ginHandler.Close()        // 关闭 gRPC 客户端连接

	if mysqlRepo != nil {
		mysqlRepo.Close()
	}
	log.Println("[main] shutdown complete")
}
