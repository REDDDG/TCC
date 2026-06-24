package repository

import (
	"context"
	"fmt"
	"tcc/internal/model"
	"time"
)

// OrderTry Order的Try尝试
func (r *MySQLRepository) OrderTry(ctx context.Context, order model.Order) error {
	cnt, err := r.db.ExecContext(ctx, "INSERT order_main(xid, user_id, product_id, quantity, amount, status) values (?,?,?,?,?,?) ",
		order.XID, order.UserID, order.ProductID, order.Quantity, order.Amount, order.Status)
	if err != nil {
		return err
	}
	nums, _ := cnt.RowsAffected()
	if nums == 1 {
		return nil
	}
	return fmt.Errorf("order try %d failed", nums)
}

func (r *MySQLRepository) OrderConfirm(ctx context.Context, order model.Order) error {
	_, err := r.db.ExecContext(ctx, "UPDATE order_main SET updated_at =? where user_id = ?", time.Now(), order.UserID)
	if err != nil {
		return err
	}
	return nil
}

// OrderCancel Order的Cancel回滚
func (r *MySQLRepository) OrderCancel(ctx context.Context, order model.Order) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM order_main where xid=?", order.XID)
	if err != nil {
		return err
	}
	return nil
}
