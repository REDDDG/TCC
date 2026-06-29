package repository

import (
	"context"
	"fmt"
	"tcc/internal/model"
	"time"
)

func (r *MySQLRepository) PointsTry(ctx context.Context, branchId int64, value int64, account model.PointsAccount) error {
	cnt, err := r.DB.ExecContext(ctx, "UPDATE points_account SET  balance = balance - ? WHERE user_id = ?", value, account.UserID)
	if err != nil {
		return err
	}
	nums, _ := cnt.RowsAffected()
	if nums == 1 {
		return nil
	}
	return fmt.Errorf("points try %d failed", nums)
}

func (r *MySQLRepository) PointsConfirm(ctx context.Context, branchId int64, account model.PointsAccount) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE points_account SET updated_at=? where user_id = ? ", time.Now(), account.UserID)
	if err != nil {
		return err
	}
	return nil
}

func (r *MySQLRepository) PointsCancel(ctx context.Context, branchId int64, account model.PointsAccount) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE points_account SET  balance = balance + 1,updated_at=? WHERE user_id = ?", time.Now(), account.UserID)
	if err != nil {
		return err
	}
	return nil
}
