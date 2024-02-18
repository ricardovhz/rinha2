package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/ricardovhz/rinha2/repository"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	repo := repository.NewTcpRepository("localhost:5001")

	for i := 1; i < 6; i++ {
		id := strconv.Itoa(i)
		r, err := repo.GetResume(context.Background(), id)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%d | %d | %v\n", r.Limit, r.Balance, r.Transactions)
	}

	// n, _ := strconv.Atoi(os.Args[1])

	// for i := 0; i < n; i++ {
	// 	lim, bal, err := repo.SaveTransaction(context.Background(), "1", &model.Transaction{
	// 		Timestamp:   time.Now().UnixMilli(),
	// 		Value:       100,
	// 		Type:        "d",
	// 		Description: "asd",
	// 	})
	// 	if err != nil {
	// 		fmt.Printf("err %v\n", err)
	// 	}
	// 	fmt.Printf("lim: %d, bal: %d\n", lim, bal)
	// }
}
