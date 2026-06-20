package recoverer

import (
	"context"
	"tcc/internal/model"
)

type Recoverer interface {
	Recover(ctx context.Context, tx *model.Transaction) error
}
