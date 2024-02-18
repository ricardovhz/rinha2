package repository

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type storeRepository struct {
	// pool      *grpcpool.Pool
	storePool []pb.StoreServiceClient
}

// GetLimitAndBalance implements Repository.
func (s *storeRepository) GetLimitAndBalance(ctx context.Context, id string) (int, int, error) {
	panic("unimplemented")
}

func (s *storeRepository) getStoreClient() pb.StoreServiceClient {
	return s.storePool[rand.Intn(len(s.storePool))]
}

func (s *storeRepository) ShutDown() {
	panic("implement me")
}

// GetResume implements Repository.
func (s *storeRepository) GetResume(ctx context.Context, id string) (*model.Resume, error) {
	store := s.getStoreClient()
	resp, err := store.GetExtract(ctx, &pb.GetExtractRequest{
		ClientId: id,
	})
	if err != nil {
		return nil, err
	}
	if resp.Limit == 0 {
		return nil, NotFound
	}
	res := &model.Resume{
		Limit:        int(resp.Limit),
		Balance:      int(resp.Balance),
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
var p = sync.Pool{
	New: func() any {
		return &pb.Transaction{}
	},
}

func (s *storeRepository) SaveTransaction(ctx context.Context, id string, t *model.Transaction) (int, int, error) {
	tr := p.Get().(*pb.Transaction)
	tr.Type = t.Type
	tr.ClientId = id
	tr.Description = t.Description
	tr.Value = int32(t.Value)

	resp, err := s.getStoreClient().Save(ctx, &pb.SaveRequest{
		Transaction: tr,
	})
	p.Put(tr)
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
	return int(resp.Limit), int(resp.Balance), nil
}

func NewStoreRepository(ctx context.Context, target string) (Repository, error) {
	pool := make([]pb.StoreServiceClient, 10)
	for i := 0; i < 10; i++ {
		conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		pool[i] = pb.NewStoreServiceClient(conn)
	}

	return &storeRepository{
		storePool: pool,
	}, nil
}
