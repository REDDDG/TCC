package repository

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"tcc/internal/model"

	"github.com/IBM/sarama"
)

// SyncMessage Kafka 消息体。
type SyncMessage struct {
	XID        string `json:"xid,omitempty"`
	BranchID   int64  `json:"branch_id"`
	Service    string `json:"service"`     // "inventory" | "order" | "points"
	Phase      string `json:"phase"`       // "try" | "confirm" | "cancel"
	ResourceID string `json:"resource_id"` // product_id / user_id
	Data       string `json:"data"`        // JSON: qty for inventory/points, order detail for order
	Version    int    `json:"version"`     // Redis 版本号，用于乐观锁
}

const topicBranchSync = "tcc.branch.sync"

// KafkaProducer 发送消息到 Kafka。
type KafkaProducer struct {
	producer sarama.SyncProducer
}

func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{producer: producer}, nil
}

func (p *KafkaProducer) Send(ctx context.Context, msg SyncMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topicBranchSync,
		Key:   sarama.StringEncoder(msg.Service),
		Value: sarama.ByteEncoder(data),
	})
	return err
}

// Close 关闭 Kafka producer。
func (p *KafkaProducer) Close() {
	if p.producer != nil {
		p.producer.Close()
	}
}

// KafkaConsumer 消费消息，延迟写入 MySQL。
type KafkaConsumer struct {
	mysql *MySQLRepository
}

func NewKafkaConsumer(mysql *MySQLRepository) *KafkaConsumer {
	return &KafkaConsumer{mysql: mysql}
}

func (c *KafkaConsumer) Start(ctx context.Context, brokers []string) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return err
	}

	partitionConsumer, err := consumer.ConsumePartition(topicBranchSync, 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}

	go func() {
		defer consumer.Close()
		defer partitionConsumer.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-partitionConsumer.Messages():
				var syncMsg SyncMessage
				if err := json.Unmarshal(msg.Value, &syncMsg); err != nil {
					log.Printf("[kafka] unmarshal error: %v", err)
					continue
				}
				if err := c.handle(ctx, syncMsg); err != nil {
					log.Printf("[kafka] handle error: %v", err)
				}
			}
		}
	}()

	return nil
}

func (c *KafkaConsumer) handle(ctx context.Context, msg SyncMessage) error {
	log.Printf("[kafka] handling: service=%s phase=%s branch=%d version=%d", msg.Service, msg.Phase, msg.BranchID, msg.Version)

	switch msg.Service {
	case "inventory":
		return c.handleInventory(ctx, msg)
	case "order":
		return c.handleOrder(ctx, msg)
	case "points":
		return c.handlePoints(ctx, msg)
	case "transaction":
		return c.handleTransaction(ctx, msg)
	}
	return nil
}

// handleInventory 使用乐观锁更新库存：WHERE version < msg.Version（允许 catch-up）。
func (c *KafkaConsumer) handleInventory(ctx context.Context, msg SyncMessage) error {
	switch msg.Phase {
	case "confirm":
		qty, _ := strconv.Atoi(msg.Data)
		result, err := c.mysql.DB.ExecContext(ctx,
			"UPDATE inventory_stock SET total = total - ?, version = ?, updated_at = NOW() WHERE product_id = ?",
			qty, msg.Version, msg.ResourceID, msg.Version)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			log.Printf("[kafka] inventory confirm: version conflict for %s (version=%d)", msg.ResourceID, msg.Version)
		}
		return nil
	}
	return nil
}

// handleOrder 处理订单的 confirm/cancel 延迟写入。
// 只填充业务字段，status 由 coordinator 通过 UpdateTransactionStatus 管理。
func (c *KafkaConsumer) handleOrder(ctx context.Context, msg SyncMessage) error {
	var order struct {
		UserID    string  `json:"user_id"`
		ProductID string  `json:"product_id"`
		Quantity  int     `json:"quantity"`
		Amount    float64 `json:"amount"`
	}
	if err := json.Unmarshal([]byte(msg.Data), &order); err != nil {
		return err
	}
	_, err := c.mysql.DB.ExecContext(ctx,
		"INSERT order_main SET user_id=?, product_id=?, quantity=?, amount=?,id=?, updated_at=NOW()",
		order.UserID, order.ProductID, order.Quantity, order.Amount, msg.BranchID)
	return err
}

// handlePoints 使用乐观锁更新积分：WHERE version < msg.Version（允许 catch-up）。
func (c *KafkaConsumer) handlePoints(ctx context.Context, msg SyncMessage) error {
	switch msg.Phase {
	case "confirm":
		qty, _ := strconv.Atoi(msg.Data)
		result, err := c.mysql.DB.ExecContext(ctx,
			"UPDATE points_account SET balance = balance - ?, version = ?, updated_at = NOW() WHERE user_id = ?",
			qty, msg.Version, msg.ResourceID, msg.Version)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			log.Printf("[kafka] points confirm: version conflict for %s (version=%d)", msg.ResourceID, msg.Version)
		}
		return nil
	}
	return nil
}

// handleTransaction 将已完成的事务从 Kafka 刷入 MySQL（global_transaction + branch_transaction）。
func (c *KafkaConsumer) handleTransaction(ctx context.Context, msg SyncMessage) error {
	var tx model.Transaction
	if err := json.Unmarshal([]byte(msg.Data), &tx); err != nil {
		return err
	}

	participants := make([]participantJSON, len(tx.Branches))
	for i, br := range tx.Branches {
		participants[i] = participantJSON{
			ServiceName:  br.ServiceName,
			ResourceData: br.ResourceData,
			Address:      br.Address,
		}
	}
	content, _ := json.Marshal(participants)

	svcName := ""
	if len(tx.Branches) > 0 {
		svcName = tx.Branches[0].ServiceName
	}

	_, err := c.mysql.DB.ExecContext(ctx,
		`REPLACE INTO global_transaction (xid, status, service_name, create_time, update_time, timeout, content)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		tx.XID, txStatusToInt[tx.Status], svcName,
		tx.CreateTime, tx.UpdateTime, tx.Timeout, string(content),
	)
	if err != nil {
		return err
	}

	for _, br := range tx.Branches {
		_, err := c.mysql.DB.ExecContext(ctx,
			`REPLACE INTO branch_transaction (branch_id, xid, resource_id, status, create_time, update_time)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			br.BranchID, tx.XID, br.ResourceData, branchStatusToInt[br.Status],
			br.CreateTime, time.Now(),
		)
		if err != nil {
			return err
		}
	}

	log.Printf("[kafka] transaction %s flushed to MySQL (%d branches)", tx.XID, len(tx.Branches))
	return nil
}
