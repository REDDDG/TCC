package repository

import (
	"context"
	"fmt"
	"tcc/internal/model"
	"time"
)

func (r *MySQLRepository) OrderTry(ctx context.Context, branchId int64, xid string, order model.Order) error {
	cnt, err := r.DB.ExecContext(ctx, "INSERT order_main(user_id, product_id, quantity, amount, status) values (?,?,?,?,?) ",
		order.UserID, order.ProductID, order.Quantity, order.Amount, order.Status)
	if err != nil {
		return err
	}
	nums, _ := cnt.RowsAffected()
	order.Id, _ = cnt.LastInsertId()
	if nums == 1 {
		return nil
	}
	return fmt.Errorf("order try %d failed", nums)
}

func (r *MySQLRepository) OrderConfirm(ctx context.Context, branchId int64, xid string, order model.Order) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE order_main SET updated_at =? where id = ?", time.Now(), order.Id)
	if err != nil {
		return err
	}
	return nil
}

func (r *MySQLRepository) OrderCancel(ctx context.Context, branchId int64, xid string, order model.Order) error {
	_, err := r.DB.ExecContext(ctx, "DELETE FROM order_main where id=?", order.Id)
	if err != nil {
		return err
	}
	return nil
}
