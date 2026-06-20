// Package coordinator 实现 TCC 事务协调器的核心逻辑，包括 gRPC 服务、
// HTTP API 层和内存存储。
package coordinator

import (
	"sync"
	"tcc/internal/model"
	"time"
)

// Store 是事务的内存存储，使用 sync.RWMutex 保证并发安全。
// 所有读写操作均通过锁保护，支持多读单写。
type Store struct {
	mu  sync.RWMutex
	txs map[string]*model.Transaction // key: XID, value: 事务对象
}

// NewStore 创建空的内存存储实例。
func NewStore() *Store {
	return &Store{txs: make(map[string]*model.Transaction)}
}

// Create 写入一个新事务到存储。
//   - tx: 待写入的事务对象，其 XID 作为唯一键
func (s *Store) Create(tx *model.Transaction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs[tx.XID] = tx
}

// Get 按 XID 查询事务。
//   - xid: 全局事务 ID
//   - 返回值: 事务对象 + 是否找到
func (s *Store) Get(xid string) (*model.Transaction, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, ok := s.txs[xid]
	return tx, ok
}

// Update 更新已存在的事务（原地修改后写回），同时刷新 UpdateTime。
//   - tx: 已修改的事务对象，通过 XID 匹配存储中的条目
func (s *Store) Update(tx *model.Transaction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx.UpdateTime = time.Now()
	s.txs[tx.XID] = tx
}

// List 返回当前存储中的所有事务，用于前端列表展示。
// 返回值为新建切片，调用方可安全遍历。
func (s *Store) List() []*model.Transaction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*model.Transaction, 0, len(s.txs))
	for _, tx := range s.txs {
		result = append(result, tx)
	}
	return result
}
