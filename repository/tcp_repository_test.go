package repository_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

func TestTcp(t *testing.T) {
	tRepo := repository.NewTcpRepository("localhost:5001")
	_, err := tRepo.GetResume(context.Background(), "6")
	if err == nil {
		t.Fail()
	}
	log.Printf("%v", err)

	res, err := tRepo.GetResume(context.Background(), "1")
	if err != nil {
		t.Fail()
	}
	log.Printf("%d | %d | %v", res.Limit, res.Balance, res.Transactions)

	lim, bal, err := tRepo.SaveTransaction(context.Background(), "1", &model.Transaction{
		Type:        "d",
		Timestamp:   time.Now().UnixMilli(),
		Value:       1000,
		Description: "test",
	})
	if err != nil {
		log.Printf("err %v", err)
		t.FailNow()
	}
	log.Printf("lim: %d, bal: %d", lim, bal)
}

func Benchmark(b *testing.B) {
	tRepo := repository.NewTcpRepository("localhost:5001")
	tr := &model.Transaction{
		Type:        "c",
		Timestamp:   time.Now().UnixMilli(),
		Value:       10,
		Description: "devolve",
	}
	for i := 0; i < b.N; i++ {
		lim, bal, err := tRepo.SaveTransaction(context.Background(), "1", tr)
		if err != nil {
			log.Printf("err %v", err)
			b.Fail()
		}
		log.Printf("lim: %d, bal: %d", lim, bal)
	}
}
