package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ricardovhz/rinha2/pb"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	db, err := sql.Open("sqlite3", "store.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = os.Remove("store.db")
	if err != nil {
		panic(err)
	}

	s := NewStoreService(context.Background(), db)
	defer s.Close()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	for i := 0; i < 150; i++ {
		resp, err := s.Save(ctx, &pb.SaveRequest{
			Transaction: &pb.Transaction{
				ClientId:    "1",
				Type:        "c",
				Description: "asd",
				Value:       100 + int32(i),
			},
		})
		require.NoError(t, err)
		require.False(t, resp.GetError())
	}
}
