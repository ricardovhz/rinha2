package repository

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/ricardovhz/rinha2/model"
	"github.com/stretchr/testify/require"
)

func TestSave(t *testing.T) {
	rc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()
	_, err := rc.FlushAll(ctx).Result()
	if err != nil {
		t.Fail()
	}

	repo := NewRedisRepository(rc)

	_, err = rc.Set(ctx, "limit:1", "100", 0).Result()
	if err != nil {
		t.Fail()
	}

	_, _, err = repo.SaveTransaction(ctx, "2", &model.Transaction{
		Type:        "c",
		Description: "teste",
		Value:       10,
	})
	require.Error(t, err)

	repo.SaveTransaction(ctx, "1", &model.Transaction{
		Type:        "c",
		Description: "teste",
		Value:       10,
	})
	l, b, err := repo.GetLimitAndBalance(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, l, 100)
	require.Equal(t, b, 10)

	_, _, err = repo.SaveTransaction(ctx, "1", &model.Transaction{
		Type:        "d",
		Description: "teste",
		Value:       100,
	})
	require.NoError(t, err)
	l, b, err = repo.GetLimitAndBalance(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, l, 100)
	require.Equal(t, b, -90)

	// limit exceeded
	_, _, err = repo.SaveTransaction(ctx, "1", &model.Transaction{
		Type:        "d",
		Description: "teste",
		Value:       500,
	})
	require.Error(t, err)

	l, b, err = repo.GetLimitAndBalance(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, l, 100)
	require.Equal(t, b, -90)

}
