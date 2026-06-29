package main

import (
	"context"
	"log"
	"net"
	"os"

	"tcc/internal/branch/order"
	"tcc/internal/repository"

	pb "tcc/api/proto/branch"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:mysql12138@tcp(127.0.0.1:3306)/tcc?parseTime=true"
	}

	mysqlRepo, err := repository.NewMySQLRepository(dsn)
	if err != nil {
		log.Fatalf("[main] MySQL not available: %v", err)
	}
	log.Println("[main] MySQL connected, tables ensured")

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 10,
	})

	producer, err := repository.NewKafkaProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatalf("[main] Kafka producer failed: %v", err)
	}
	defer producer.Close()
	log.Println("[main] Kafka producer connected")

	repo := repository.NewRedisRepository(mysqlRepo, rdb, producer)

	consumer := repository.NewKafkaConsumer(mysqlRepo)
	go func() {
		if err := consumer.Start(context.Background(), []string{"localhost:9092"}); err != nil {
			log.Printf("[main] Kafka consumer error: %v", err)
		}
	}()

	s := grpc.NewServer()
	pb.RegisterBranchServiceServer(s, order.NewServer("OrderService", repo))

	log.Println("Order service listening on :8081")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
