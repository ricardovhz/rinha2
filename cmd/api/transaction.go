package main

import (
	"context"
	"log/slog"

	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

var (
	repo repository.Repository
)

func getRepository() repository.Repository {
	return repo
}

func saveTransaction(ctx context.Context, id string, t *model.Transaction) (int, int, error) {
	repo := getRepository()

	slog.Debug("Saving transaction for client", "description", t.Description, "client", id, "value", t.Value, "type", t.Type)
	return repo.SaveTransaction(ctx, id, t)
}

func getResume(ctx context.Context, id string) (*model.Resume, error) {
	repo := getRepository()
	return repo.GetResume(ctx, id)
}
