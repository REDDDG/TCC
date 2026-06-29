// 协调器入口：启动 gRPC server (:9090) 和 Gin HTTP server (:8080)。
// gRPC 负责 TCC 事务协议编排；Gin 提供前端页面和 REST API。
//
// 热路径走 Redis（事务 CRUD + 业务 Try/Confirm/Cancel），
// 终态通过 Kafka 异步刷入 MySQL。
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	coordpb "tcc/api/proto/coordinator"
	"tcc/internal/coordinator"
	"tcc/internal/middleware"
	"tcc/internal/myTime"
	"tcc/internal/recoverer"
	"tcc/internal/repository"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	// ── MySQL 持久化层 ──
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:mysql12138@tcp(127.0.0.1:3306)/tcc?parseTime=true"
	}

	mysqlRepo, err := repository.NewMySQLRepository(dsn)
	if err != nil {
		log.Fatalf("[main] MySQL not available: %v", err)
	}
	log.Println("[main] MySQL connected, tables ensured")

	// ── Redis 热路径 ──
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 10,
	})
	log.Println("[main] Redis connected")

	// ── Kafka ──
	producer, err := repository.NewKafkaProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatalf("[main] Kafka producer failed: %v", err)
	}
	defer producer.Close()
	log.Println("[main] Kafka producer connected")

	repo := repository.NewRedisRepository(mysqlRepo, rdb, producer)

	// Kafka consumer：消费 transaction complete 消息刷入 MySQL
	consumer := repository.NewKafkaConsumer(mysqlRepo)
	go func() {
		if err := consumer.Start(context.Background(), []string{"localhost:9092"}); err != nil {
			log.Printf("[main] Kafka consumer error: %v", err)
		}
	}()

	// ── 共享存储层（Redis 热路径 + 内存缓存）──
	store := coordinator.NewStore(repo)

	// ── gRPC Server：监听 :9090 ──
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
	// Scanner 定时扫描 Redis 中已超时的非终态事务，
	// 交给 Recoverer 执行补偿 Cancel。
	scannerCancel := func() {}
	rec := recoverer.NewDefaultRecoverer(repo)
	scanner := recoverer.NewTimeoutScanner(repo, rec, 10*myTime.MyTime)

	var scannerCtx context.Context
	scannerCtx, scannerCancel = context.WithCancel(context.Background())

	go func() {
		if err := scanner.Run(scannerCtx); err != nil {
			log.Printf("[main] scanner exited: %v", err)
		}
	}()

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
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[main] shutting down...")
	scannerCancel()
	grpcServer.GracefulStop()
	ginHandler.Close()

	mysqlRepo.Close()
	repo.Close()
	log.Println("[main] shutdown complete")
}
