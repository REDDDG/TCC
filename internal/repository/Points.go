package repository

import (
	"context"
	"fmt"
	"tcc/internal/model"
	"time"
)

// PointsTry Points的Try尝试
func (r *MySQLRepository) PointsTry(ctx context.Context, account model.PointsAccount) error {
	cnt, err := r.db.ExecContext(ctx, "UPDATE points_account SET  balance = balance - 1 WHERE user_id = ? AND balance >= 1", account.UserID)
	if err != nil {
		return err
	}
	nums, _ := cnt.RowsAffected()
	if nums == 1 {
		return nil
	}
	return fmt.Errorf("points try %d failed", nums)
}

// PointsConfirm 修改更新时间
func (r *MySQLRepository) PointsConfirm(ctx context.Context, account model.PointsAccount) error {
	_, err := r.db.ExecContext(ctx, "UPDATE points_account SET updated_at=? where user_id = ? ", time.Now(), account.UserID)
	if err != nil {
		return err
	}
	return nil
}

// PointsCancel Points的Cancel回滚
func (r *MySQLRepository) PointsCancel(ctx context.Context, account model.PointsAccount) error {
	_, err := r.db.ExecContext(ctx, "UPDATE points_account SET  balance = balance + 1,updated_at=? WHERE user_id = ?", time.Now(), account.UserID)
	if err != nil {
		return err
	}
	return nil
}
