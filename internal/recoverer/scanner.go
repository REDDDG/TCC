package recoverer

import (
	"context"
	"log"
	"tcc/internal/model"
	"time"

	"tcc/internal/repository"
)

// TimeoutScanner 定时扫描 MySQL 中已超时的非终态事务，交给 Recoverer 处理。
//
// 意义：协调器可能宕机、网络分区，或业务方在 Begin 成功后未发起 Commit/Cancel。
// Scanner 作为后台巡检机制，确保这些"悬挂"事务不会无限期占用资源。
// 扫描间隔默认 10 秒——事务超时通常 30 秒，10 秒粒度足以在 30~40 秒内介入。
type TimeoutScanner struct {
	repo      repository.Repository
	recoverer Recoverer
	interval  time.Duration
}

// NewTimeoutScanner 创建超时扫描器。
//
//	repo:      仓储（通常为 MySQL 实现），用于 FindTimeOutTransactions
//	recoverer: 恢复器，对每个超时事务执行恢复逻辑
//	interval:  扫描间隔，若 <= 0 则默认 10 秒
func NewTimeoutScanner(repo repository.Repository, recoverer Recoverer, interval time.Duration) *TimeoutScanner {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &TimeoutScanner{
		repo:      repo,
		recoverer: recoverer,
		interval:  interval,
	}
}

// Run 启动扫描循环，直到 ctx 被取消。
//
// 意义：Run 是阻塞方法，应在独立 goroutine 中调用。
// 每次扫描周期：
//  1. 查询超时事务列表（repo.FindTimeOutTransactions）
//  2. 对每个超时事务调用 recoverer.Recover
//  3. 不管 Recover 成功与否都继续处理下一个（fail-open 策略）
//  4. 等待 interval 后再次扫描
//
// 使用 time.Ticker 以固定间隔触发，而非 sleep——避免某次恢复耗时过长导致间隔漂移。
func (s *TimeoutScanner) Run(ctx context.Context) error {
	log.Printf("[scanner] started, interval=%v", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[scanner] stopped")
			return ctx.Err()
		case <-ticker.C:
			s.scanOnce()
		}
	}
}

// scanOnce 执行一次完整的扫描-恢复周期。
func (s *TimeoutScanner) scanOnce() {
	txs, err := s.repo.FindTimeOutTransactions(context.Background())
	if err != nil {
		log.Printf("[scanner] FindTimeOutTransactions failed: %v", err)
		return
	}

	if len(txs) == 0 {
		return
	}

	log.Printf("[scanner] found %d timed-out transaction(s)", len(txs))
	for _, tx := range txs {
		// 每个事务用独立 context 防止级联超时
		if tx.RetryCount < 5 && tx.Status == model.StatusConfirming {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.recoverer.Recover(ctx, tx); err != nil {
				log.Printf("[scanner] Recover failed for tx %s: %v", tx.XID, err)
			}
			cancel()
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.recoverer.Cancel(ctx, tx); err != nil {
				log.Printf("[scanner] cancel XID=%s failed: %v", tx.XID, err)
			}
			cancel()
		}
	}
}
