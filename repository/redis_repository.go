package repository

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ricardovhz/rinha2/model"
)

type redisRepository struct {
	redisClient *redis.Client
}

func (r *redisRepository) ShutDown() {
	r.redisClient.Close()
}

func (r *redisRepository) GetLimitAndBalance(ctx context.Context, id string) (int, int, error) {

	var (
		rl      string
		limit   int
		balance int
	)
	for {
		cmds, err := r.redisClient.Pipelined(ctx, func(p redis.Pipeliner) error {
			p.Get(ctx, "readlock:"+id)
			p.Get(context.Background(), "limit:"+id)
			p.Get(context.Background(), "balance:"+id)
			return nil
		})
		if err != nil && cmds[1].Err() != nil {
			slog.Error("Error getting limit", "error", err, "client", id)
			return -1, -1, ErrClientNotInitialized
		}
		rl, _ = cmds[0].(*redis.StringCmd).Result()
		limit, _ = cmds[1].(*redis.StringCmd).Int()
		balance, _ = cmds[2].(*redis.StringCmd).Int()
		if rl == "1" || balance < limit*-1 {

			// transacao em andamento, aguardando liberação
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Nanosecond)
		} else {
			break
		}
	}
	return limit, balance, nil
}

func (r *redisRepository) SaveTransaction(ctx context.Context, id string, t *model.Transaction) (int, int, error) {
	var (
		err   error
		limit int
		total int64
		index int64 = -1
	)
	c := r.redisClient
	p := c.Pipeline()

	p.Get(ctx, "limit:"+id)
	p.Incr(ctx, "transaction-index:"+id)
	p.SetNX(ctx, "balance:"+id, 0, 0)
	p.Get(ctx, "balance:"+id)
	cmds, err := p.Exec(ctx)
	if err != nil {
		slog.Error("Error getting limit and balance", "error", err, "description", t.Description, "client", id, "cmds", cmds[0])
		return -1, -1, err
	} else {
		limit, _ = cmds[0].(*redis.StringCmd).Int()
		index, _ = cmds[1].(*redis.IntCmd).Result()
		total, _ = cmds[3].(*redis.StringCmd).Int64()
	}

	for {
		// se valor a debitar for excedido do limite
		if t.Type == "d" && int(total)+t.GetValue() < limit*-1 {
			slog.Info("Limit exceeded", "limit", limit, "balance", total, "description", t.Description, "client", id)
			return -1, -1, ErrLimitExceeded
		}

		index = (index - 1) % 5
		for {
			_, err = c.Set(ctx, fmt.Sprintf("transaction:%s:%d", id, index), fmt.Sprintf("%s;%s;%s;%d", time.Now().Format(time.RFC3339Nano), t.Type, t.Description, t.Value), 0).Result()
			if err != nil {
				slog.Error("Error saving transaction", "error", err, "description", t.Description, "client", id)
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Nanosecond)
			} else {
				break
			}
		}

		validated := true
		for {
			rl, err := p.SetArgs(ctx, "readlock:"+id, "1", redis.SetArgs{
				Get: true,
			}).Result()
			if err != nil {
				slog.Warn("Error setting readlock", "error", err, "description", t.Description, "client", id)
			}
			if rl == "1" {
				slog.Warn("Transaction in progress. waiting", "description", t.Description, "client", id)
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
			} else {
				break
			}
		}

		for {
			totalVal, _ := c.Get(ctx, "balance:"+id).Int64()
			if totalVal != total {
				slog.Warn("Balance changed. restarting", "old", total, "new", totalVal, "description", t.Description, "client", id)
				validated = false
				break
			}
			total, err = c.IncrBy(ctx, "balance:"+id, int64(t.GetValue())).Result()
			if err != nil {
				slog.Error("Error incrementing balance", "error", err, "description", t.Description, "client", id)
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Nanosecond)
			} else {
				break
			}
		}

		if !validated {

			// must restart transaction
			time.Sleep(time.Duration(rand.Intn(80)) * time.Microsecond)
			total, _ = c.Get(ctx, "balance:"+id).Int64()
			continue
		}

		if int(total) < limit*-1 {
			slog.Warn("[inconsistency] Limit exceeded", "limit", limit, "balance", total, "description", t.Description, "client", id)
			for {
				total, err = c.IncrBy(ctx, "balance:"+id, int64(t.GetValue()*-1)).Result()
				if err != nil {
					slog.Error("Error returning back balance", "error", err, "description", t.Description, "client", id)
					time.Sleep(time.Duration(rand.Intn(80)) * time.Microsecond)
				} else {
					break
				}
			}
			time.Sleep(time.Duration(rand.Intn(80)) * time.Microsecond)
			continue
		}

		// tudo ok
		c.Set(ctx, "readlock:"+id, "0", 0)
		return limit, int(total), nil
	}
}

func (r *redisRepository) GetResume(ctx context.Context, id string) (*model.Resume, error) {
	limit, balance, err := r.GetLimitAndBalance(ctx, id)
	if err != nil {
		return nil, ErrClientNotInitialized
	}

	// values, err := r.redisClient.ZRevRange(ctx, "transactions:"+id, 0, 5).Result()
	// if err != nil {
	// 	slog.Error("Error getting id", "id", id)
	// 	return nil, NotFound
	// }
	res := &model.Resume{
		Balance:      balance,
		Limit:        limit,
		Transactions: make([]*model.Transaction, 0),
	}

	for i := 0; i < 5; i++ {
		values, err := r.redisClient.Get(ctx, fmt.Sprintf("transaction:%s:%d", id, i)).Result()
		if err != nil {
			continue
		}
		spl := strings.Split(values, ";")
		var t model.Transaction
		t.Date = spl[0]
		t.Type = spl[1]
		t.Description = spl[2]
		t.Value, _ = strconv.Atoi(spl[3])
		res.Transactions = append(res.Transactions, &t)
	}
	sort.SliceStable(res.Transactions, func(i, j int) bool {
		return res.Transactions[i].Date > res.Transactions[j].Date
	})
	return res, nil
}

func NewRedisRepository(redisClient *redis.Client) Repository {
	return &redisRepository{
		redisClient: redisClient,
	}
}
