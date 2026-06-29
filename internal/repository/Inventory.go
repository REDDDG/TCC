package repository

import (
	"context"
	"fmt"
	"time"
)

func (r *MySQLRepository) InventoryTry(ctx context.Context, branchId int64, value int64, productId string) error {
	cnt, err := r.DB.ExecContext(ctx, "UPDATE inventory_stock SET total =total- ? where product_id=?", value, productId)
	if err != nil {
		return err
	}
	nums, _ := cnt.RowsAffected()
	if nums == 1 {
		return nil
	}
	return fmt.Errorf("inventory try %d failed", nums)
}

func (r *MySQLRepository) InventoryConfirm(ctx context.Context, branchId int64, productId string) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE inventory_stock SET updated_at=? where product_id=?", time.Now(), productId)
	if err != nil {
		return err
	}
	return nil
}

func (r *MySQLRepository) InventoryCancel(ctx context.Context, branchId int64, productId string) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE inventory_stock SET total=total+ 1 where product_id=?", productId)
	if err != nil {
		return err
	}
	return nil
}
