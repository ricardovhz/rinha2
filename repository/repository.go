package repository

import (
	"context"

	"github.com/ricardovhz/rinha2/model"
)

type Repository interface {
	GetLimitAndBalance(ctx context.Context, id string) (int, int, error)
	SaveTransaction(ctx context.Context, id string, t model.Transaction) (int, int, error)
	GetResume(ctx context.Context, id string) (*model.Resume, error)
}
