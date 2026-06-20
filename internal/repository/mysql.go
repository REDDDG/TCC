package repository

import "tcc/internal/model"

var txStatusToInt = map[model.TxStatus]int{
	model.StatusTrying:     0,
	model.StatusConfirming: 1,
	model.StatusCancelling: 2,
	model.StatusCompleted:  3,
	model.StatusFailed:     4,
}
