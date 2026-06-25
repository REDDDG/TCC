package repository

import (
	"context"
	"fmt"
	"time"
)

// InventoryTry Inventory的Try尝试
func (r *MySQLRepository) InventoryTry(ctx context.Context, productId string) error {
	cnt, err := r.db.ExecContext(ctx, "UPDATE inventory_stock SET total =total- 1 where product_id=? AND total>=1", productId)
	if err != nil {
		return err
	}
	nums, _ := cnt.RowsAffected()
	if nums == 1 {
		return nil
	}
	return fmt.Errorf("inventory try %d failed", nums)
}

func (r *MySQLRepository) InventoryConfirm(ctx context.Context, productId string) error {
	_, err := r.db.ExecContext(ctx, "UPDATE inventory_stock SET updated_at=? where product_id=?", time.Now(), productId)
	if err != nil {
		return err
	}
	return nil
}

func (r *MySQLRepository) InventoryCancel(ctx context.Context, productId string) error {
	_, err := r.db.ExecContext(ctx, "UPDATE inventory_stock SET total=total+ 1 where product_id=?", productId)
	if err != nil {
		return err
	}
	return nil
}
