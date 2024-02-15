package main

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

var (
	repo     repository.Repository
	repoOnce sync.Once
)

func getRepository() repository.Repository {
	repoOnce.Do(func() {
		// redisClient := redis.NewClient(&redis.Options{
		// 	Addr:     "localhost:6379",
		// 	PoolSize: 5 * runtime.GOMAXPROCS(0),
		// })
		// redisClient.FlushAll(context.Background())

		// _, err := redisClient.Pipelined(context.Background(), func(p redis.Pipeliner) error {
		// 	p.Set(context.Background(), "limit:1", 100000, 0)
		// 	p.Set(context.Background(), "balance:1", 0, 0)

		// 	p.Set(context.Background(), "limit:2", 80000, 0)
		// 	p.Set(context.Background(), "balance:2", 0, 0)

		// 	p.Set(context.Background(), "limit:3", 1000000, 0)
		// 	p.Set(context.Background(), "balance:3", 0, 0)

		// 	p.Set(context.Background(), "limit:4", 10000000, 0)
		// 	p.Set(context.Background(), "balance:4", 0, 0)

		// 	p.Set(context.Background(), "limit:5", 500000, 0)
		// 	p.Set(context.Background(), "balance:5", 0, 0)

		// 	return nil
		// })
		// if err != nil {
		// 	panic(err)
		// }

		// repo = repository.NewRedisRepository(redisClient)

	})
	return repo
}

func saveTransaction(ctx context.Context, id string, t model.Transaction) (int, int, error) {
	repo := getRepository()
	// limit, balance, err := repo.GetLimitAndBalance(context.Background(), id)
	// if err != nil {
	// 	return -1, -1, err
	// }

	// // valida transacao
	// futureBalance := balance + t.GetValue()
	// if futureBalance*-1 > limit {

	// 	return -1, -1, ErrLimitExceeded
	// }

	slog.Debug("Saving transaction for client", "description", t.Description, "client", id, "value", t.Value, "type", t.Type)
	return repo.SaveTransaction(ctx, id, t)

}

func getResume(ctx context.Context, id string) (*model.Resume, error) {
	repo := getRepository()
	return repo.GetResume(ctx, id)
}
