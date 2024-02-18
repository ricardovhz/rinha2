package repository_test

import (
	"context"
	"testing"

	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

func TestStoreRepository(t *testing.T) {
	repo, err := repository.NewStoreRepository(context.Background(), "localhost:5001")
	if err != nil {
		t.Fail()
	}

	tr := model.Transaction{
		Type:        "c",
		Date:        "2021-01-01",
		Value:       100,
		Description: "test",
	}

	_, _, err = repo.SaveTransaction(context.Background(), "1", &tr)
	if err != nil {
		t.Fail()
	}
}

func BenchmarkStoreRepository(b *testing.B) {
	repo, err := repository.NewStoreRepository(context.Background(), "localhost:5001")
	if err != nil {
		b.Fail()
	}

	tr := &model.Transaction{
		Type:        "c",
		Date:        "2021-01-01",
		Value:       100,
		Description: "test",
	}

	for i := 0; i < b.N; i++ {
		_, _, err = repo.SaveTransaction(context.Background(), "1", tr)
		if err != nil {
			b.Fail()
		}
	}
}
