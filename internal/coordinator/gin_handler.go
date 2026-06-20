package coordinator

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	coordpb "tcc/api/proto/coordinator"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GinHandler 提供 HTTP REST API，作为前端与 gRPC 协调器之间的适配层。
// 创建/提交/查询等操作通过 gRPC 转发给协调器；列表操作直接读内存 Store 以减少一跳。
type GinHandler struct {
	coordAddr string // 协调器 gRPC 地址，如 "localhost:9090"
	store     *Store // 内存存储引用，用于直接读取事务列表
	conn      *grpc.ClientConn
	client    coordpb.TCCCoordinatorClient
}

// NewGinHandler 创建 HTTP handler 实例。
//   - coordAddr: 协调器 gRPC 监听地址
//   - store: 与 gRPC 协调器共享的内存存储
func NewGinHandler(coordAddr string, store *Store) *GinHandler {
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	client := coordpb.NewTCCCoordinatorClient(conn)
	return &GinHandler{coordAddr: coordAddr, store: store, conn: conn, client: client}
}

// CreateTransaction 处理 POST /api/v1/transactions
// 从 JSON body 解析参与者列表，转为 proto 结构后通过 gRPC 发起 Begin。
//   - c: Gin 上下文，请求体含 participants 数组和可选的 timeout
//   - 返回: JSON 格式的 BeginResponse（含 xid、success、branch_results）
func (h *GinHandler) CreateTransaction(c *gin.Context) {
	var req struct {
		Participants []struct {
			ServiceName  string `json:"service_name"`
			ResourceData string `json:"resource_data"`
			Address      string `json:"address"`
		} `json:"participants"`
		Timeout int32 `json:"timeout"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	participants := make([]*coordpb.Participant, len(req.Participants))
	for i, p := range req.Participants {
		participants[i] = &coordpb.Participant{
			ServiceName:  p.ServiceName,
			ResourceData: p.ResourceData,
			Address:      p.Address,
		}
	}
	start := time.Now()

	resp, err := h.client.Begin(ctx, &coordpb.BeginRequest{
		Participants: participants,
		Timeout:      req.Timeout,
	})
	end := time.Since(start)
	fmt.Println("1:", end)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// ListTransactions 处理 GET /api/v1/transactions
// 直接读取内存 Store，不走 gRPC，返回事务摘要列表（XID、状态、分支数、创建时间）。
//   - c: Gin 上下文
//   - 返回: JSON 数组，每项为事务摘要对象
func (h *GinHandler) ListTransactions(c *gin.Context) {
	txs := h.store.List()
	type txSummary struct {
		XID        string `json:"xid"`
		Status     string `json:"status"`
		BranchCnt  int    `json:"branch_count"`
		CreateTime string `json:"create_time"`
	}
	result := make([]txSummary, len(txs))
	for i, tx := range txs {
		result[i] = txSummary{
			XID:        tx.XID,
			Status:     string(tx.Status),
			BranchCnt:  len(tx.Branches),
			CreateTime: tx.CreateTime.Format(time.RFC3339),
		}
	}
	c.JSON(http.StatusOK, result)
}

// GetTransaction 处理 GET /api/v1/transactions/:xid
// 通过 gRPC GetStatus 查询事务详情，包含全局状态和所有分支状态。
//   - c: Gin 上下文，URL 参数 xid 指定事务 ID
//   - 返回: JSON 格式的 StatusResponse
func (h *GinHandler) GetTransaction(c *gin.Context) {
	xid := c.Param("xid")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := h.client.GetStatus(ctx, &coordpb.StatusRequest{Xid: xid})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// CommitTransaction 处理 POST /api/v1/transactions/:xid/commit
// 通过 gRPC Commit 确认提交全局事务。
//   - c: Gin 上下文，URL 参数 xid 指定事务 ID
//   - 返回: JSON 格式的 CommitResponse
func (h *GinHandler) CommitTransaction(c *gin.Context) {
	xid := c.Param("xid")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := h.client.Commit(ctx, &coordpb.CommitRequest{Xid: xid})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// HealthCheck 处理 GET /api/v1/health
// 返回协调器健康状态，用于前端健康栏展示。
//   - c: Gin 上下文
//   - 返回: JSON 含 status("ok")、role("leader")、time
func (h *GinHandler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"role":   "leader",
		"time":   time.Now().Format(time.RFC3339),
	})
}
