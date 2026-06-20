// Package coordinator 实现 TCC 事务协调器的核心逻辑，包括 gRPC 服务、
// HTTP API 层和持久化存储缓存。
package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	"tcc/internal/model"
	"tcc/internal/repository"
)

// Store 是事务的持久化存储层，内部维护 MySQL 持久化 + 内存缓存。
//
// 写入策略（write-through）：先写 MySQL，成功后更新内存缓存。
// 读取策略（cache-aside）：先查内存缓存，miss 时回源 MySQL 并回填缓存。
// 并发安全由 sync.RWMutex 保护缓存操作，MySQL 层由连接池保证线程安全。
type Store struct {
	mu   sync.RWMutex
	txs  map[string]*model.Transaction // key: XID, 内存缓存
	repo repository.Repository         // MySQL 持久化层
}

// NewStore 创建带 MySQL 持久化的 Store。
//
//	repo: Repository 实现（通常为 *MySQLRepository）
//
// 意义：构造函数注入依赖，Store 不关心底层是 MySQL 还是其他存储，
// 符合依赖倒置原则。若 repo 为 nil 则退化为纯内存模式（Phase 1 兼容）。
func NewStore(repo repository.Repository) *Store {
	return &Store{
		txs:  make(map[string]*model.Transaction),
		repo: repo,
	}
}

// dbCtx 为仓储操作生成独立超时 context，避免调用方取消影响持久化。
//
// 意义：每个 DB 操作独立 3 秒超时。即使上游 gRPC handler 的 context
// 已被取消（例如客户端断开），写操作仍有机会完成——这对事务一致性至关重要。
func dbCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

// Create 写入一个新事务：先持久化到 MySQL（含所有分支），再缓存到内存。
//
// 重要：CreateTransaction 会回填分支的 BranchID（MySQL AUTO_INCREMENT），
// 调用方后续通过 branch.BranchID 引用分支。
func (s *Store) Create(tx *model.Transaction) {
	// 持久化
	if s.repo != nil {
		ctx, cancel := dbCtx()
		defer cancel()
		if err := s.repo.CreateTransaction(ctx, tx); err != nil {
			log.Printf("[store] CreateTransaction %s failed: %v", tx.XID, err)
			return // 持久化失败则不缓存，避免缓存与 DB 不一致
		}
	}
	// 缓存
	s.mu.Lock()
	s.txs[tx.XID] = tx
	s.mu.Unlock()
}

// Get 按 XID 查询事务：先查内存缓存，miss 时回源 MySQL。
//
//	返回值: 事务对象 + 是否找到
func (s *Store) Get(xid string) (*model.Transaction, bool) {
	// 先查缓存
	s.mu.RLock()
	tx, ok := s.txs[xid]
	s.mu.RUnlock()
	if ok {
		return tx, true
	}

	// 缓存 miss，回源 MySQL
	if s.repo == nil {
		return nil, false
	}
	ctx, cancel := dbCtx()
	defer cancel()
	tx, err := s.repo.GetTransaction(ctx, xid)
	if err != nil {
		log.Printf("[store] GetTransaction %s failed: %v", xid, err)
		return nil, false
	}
	if tx == nil {
		return nil, false
	}

	// 回填缓存
	s.mu.Lock()
	s.txs[xid] = tx
	s.mu.Unlock()
	return tx, true
}

// Update 持久化全局事务状态及所有分支状态的变化，同时刷新内存缓存。
//
// 意义：Update 是事务状态机的"提交点"——每次状态转换（Trying→Confirming→Completed）
// 都经过此方法确保 MySQL 与内存一致。
// 全局状态和分支状态分别通过 UpdateTransactionStatus / UpdateBranchStatus
// 单独更新，避免全量覆盖。
func (s *Store) Update(tx *model.Transaction) {
	tx.UpdateTime = time.Now()

	if s.repo != nil {
		ctx, cancel := dbCtx()
		defer cancel()

		if err := s.repo.UpdateTransactionStatus(ctx, tx.XID, tx.Status); err != nil {
			log.Printf("[store] UpdateTransactionStatus %s failed: %v", tx.XID, err)
			return
		}
		for _, br := range tx.Branches {
			if err := s.repo.UpdateBranchStatus(ctx, br.BranchID, br.Status); err != nil {
				log.Printf("[store] UpdateBranchStatus %s(branch=%d) failed: %v",
					tx.XID, br.BranchID, err)
			}
		}
	}

	// 更新缓存
	s.mu.Lock()
	s.txs[tx.XID] = tx
	s.mu.Unlock()
}

// List 返回所有事务的摘要列表，直接从 MySQL 查询以保证数据一致性。
//
// 意义：列表数据由 MySQL ORDER BY 保证时间排序，内存缓存无法提供可靠的顺序。
// 返回值为新建切片，调用方可安全遍历和修改。
func (s *Store) List() []*model.Transaction {
	if s.repo == nil {
		// 纯内存降级
		s.mu.RLock()
		defer s.mu.RUnlock()
		result := make([]*model.Transaction, 0, len(s.txs))
		for _, tx := range s.txs {
			result = append(result, tx)
		}
		return result
	}

	ctx, cancel := dbCtx()
	defer cancel()
	txs, err := s.repo.ListTransactions(ctx)
	if err != nil {
		log.Printf("[store] ListTransactions failed: %v", err)
		return nil
	}
	return txs
}

// LoadBranchDetails 补充事务的全部分支详情（service_name、address 等），
// 从 MySQL content 字段反序列化参与者信息。
//
// 意义：ListTransactions 返回的摘要不含完整分支信息，
// 前端详情页面需要此方法补充。接收方负责确保 tx 非 nil。
func (s *Store) LoadBranchDetails(xid string) (*model.Transaction, bool) {
	if s.repo == nil {
		return s.Get(xid)
	}
	ctx, cancel := dbCtx()
	defer cancel()
	tx, err := s.repo.GetTransaction(ctx, xid)
	if err != nil {
		log.Printf("[store] LoadBranchDetails %s failed: %v", xid, err)
		return nil, false
	}
	return tx, tx != nil
}

func (s *Store) ClearAllTransactions() error {
	err := s.repo.ClearAllTransactions(context.Background())
	if err != nil {
		return err
	}
	return nil
}
