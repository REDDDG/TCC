package recoverer

import "context"

type Scanner interface {
	Run(ctx context.Context) error
}
