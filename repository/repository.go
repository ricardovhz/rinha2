package repository

import (
	"context"
	"errors"

	"github.com/ricardovhz/rinha2/model"
)

var (
	ErrClientNotInitialized = errors.New("client not initialized")
	ErrLimitExceeded        = errors.New("limit exceeded")
)

type Repository interface {
	GetLimitAndBalance(ctx context.Context, id string) (int, int, error)
	SaveTransaction(ctx context.Context, id string, t *model.Transaction) (int, int, error)
	GetResume(ctx context.Context, id string) (*model.Resume, error)
	ShutDown()
}
