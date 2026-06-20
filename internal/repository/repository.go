package repository

import (
	"context"
	"tcc/internal/model"
)

type Repository interface {
	CreateTransaction(ctx context.Context, tx *model.Transaction) error
	GetTransactions(ctx context.Context) (*model.Transaction, error)
	UpdateTransaction(ctx context.Context, xid string, status *model.Transaction) error

	CreateBranches(ctx context.Context, xid string, branches []*model.BranchTransaction) error
	UpdateBranchesStatus(ctx context.Context, branchID int64, status *model.BranchTransaction) error
	FindTimeOutTransactions(ctx context.Context, xid string) ([]*model.Transaction, error)
	ListTransactions(ctx context.Context) ([]*model.Transaction, error)
}
