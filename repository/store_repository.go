package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type storeRepository struct {
	con   *grpc.ClientConn
	store pb.StoreServiceClient
}

// GetLimitAndBalance implements Repository.
func (s *storeRepository) GetLimitAndBalance(ctx context.Context, id string) (int, int, error) {
	panic("unimplemented")
}

// GetResume implements Repository.
func (s *storeRepository) GetResume(ctx context.Context, id string) (*model.Resume, error) {
	resp, err := s.store.GetExtract(ctx, &pb.GetExtractRequest{
		ClientId: id,
	})
	if err != nil {
		return nil, err
	}
	if resp.LimitAndBalance == nil {
		return nil, NotFound
	}
	res := &model.Resume{
		Limit:        int(resp.LimitAndBalance.Limit),
		Balance:      int(resp.LimitAndBalance.Balance),
		Transactions: make([]*model.Transaction, len(resp.Transactions)),
	}
	for i, t := range resp.Transactions {
		tt := &model.Transaction{
			Type:        t.Type,
			Date:        t.Date,
			Description: t.Description,
			Value:       int(t.Value),
		}
		res.Transactions[i] = tt
	}
	return res, nil
}

// SaveTransaction implements Repository.
func (s *storeRepository) SaveTransaction(ctx context.Context, id string, t model.Transaction) (int, int, error) {
	resp, err := s.store.Save(ctx, &pb.SaveRequest{
		Transaction: &pb.Transaction{
			Type:        t.Type,
			ClientId:    id,
			Description: t.Description,
			Value:       int32(t.Value),
		},
	})
	if err != nil {
		return -1, -1, err
	}
	if resp.Error {
		if strings.Contains(resp.Message, "exceeded") {
			return -1, -1, ErrLimitExceeded
		} else if strings.ContainsAny(resp.Message, "initialized") {
			return -1, -1, NotFound
		}
		return -1, -1, fmt.Errorf("error saving transaction: %v", resp.Message)
	}
	return int(resp.LimitAndBalance.Limit), int(resp.LimitAndBalance.Balance), nil
}

func NewStoreRepository(ctx context.Context, host string, port int32) (Repository, error) {
	con, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", host, port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	store := pb.NewStoreServiceClient(con)
	return &storeRepository{
		con:   con,
		store: store,
	}, nil
}
