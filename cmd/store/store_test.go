package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ricardovhz/rinha2/db"
	"github.com/ricardovhz/rinha2/model"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	dba := db.NewDB(db.NewFileWriterFactoryFromPath("./"), nil)
	s := NewStoreService(context.Background(), dba)
	defer s.Close()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	s.InitializeClient("1", 100000, 0)

	for i := 0; i < 4; i++ {
		_, _, err := s.Save(ctx, db.ToRecord("1", &model.Transaction{
			Type:        "c",
			Description: "asd",
			Value:       100 + i,
		}))
		require.NoError(t, err)
	}
}

func BenchmarkStore(b *testing.B) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		// Level: slog.LevelDebug,
	})))

	dba := db.NewDB(db.NewFileWriterFactoryFromPath("./"), nil)
	s := NewStoreService(context.Background(), dba)
	defer s.Close()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	s.InitializeClient("1", 100000, 0)
	s.InitializeClient("2", 80000, 0)
	s.InitializeClient("3", 1000000, 0)
	s.InitializeClient("4", 10000000, 0)
	s.InitializeClient("5", 500000, 0)

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			id := fmt.Sprintf("%d", rand.Intn(5)+1)
			ti := rand.Intn(10000) % 2
			t := "c"
			if ti == 1 {
				t = "d"
			}

			s.Save(ctx, db.ToRecord(id, &model.Transaction{
				Type:        t,
				Description: "asd",
				Value:       10000,
			}))
			// if r.Error {
			// 	slog.Error("Error saving transaction", "id", id, "error", r.Message)
			// }
		}
	})

	// for i := 1; i <= 5; i++ {
	// 	ex, _ := s.GetExtract(ctx, &pb.GetExtractRequest{ClientId: fmt.Sprintf("%d", i)})
	// 	slog.Info("extract", "id", i, "ex", ex)
	// }
}
