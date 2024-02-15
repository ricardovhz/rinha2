package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

func main() {
	fmt.Printf("GOMAXPROCS is %d\n", runtime.GOMAXPROCS(0))
	var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		// Level: slog.LevelDebug,
		Level: slog.LevelWarn,
	}))
	slog.SetDefault(logger)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	storeRepo, err := repository.NewStoreRepository(ctx, "localhost", 5001)
	if err != nil {
		panic(err)
	}
	repo = storeRepo

	gin.DisableConsoleColor()
	f, _ := os.Create("gin.log")
	gin.DefaultWriter = io.MultiWriter(f)

	r := gin.Default()

	// POST /clientes/[id]/transacoes
	r.POST("/clientes/:id/transacoes", func(gctx *gin.Context) {
		var t model.Transaction
		id := gctx.Param("id")
		err := gctx.ShouldBindJSON(&t)
		if err != nil {
			slog.Error("Error binding json", "error", err, "id", id)
			gctx.JSON(http.StatusUnprocessableEntity, gin.H{"message": err.Error()})
			return
		}

		err = t.Validate()
		if err != nil {
			slog.Error("Error validating json", "error", err, "id", id)
			gctx.JSON(http.StatusUnprocessableEntity, gin.H{"message": err.Error()})
			return
		}

		limit, balance, err := saveTransaction(ctx, id, t)
		if err != nil {
			switch err {
			case repository.ErrLimitExceeded:
				gctx.JSON(http.StatusUnprocessableEntity, gin.H{"message": err.Error()})
			case repository.NotFound:
				gctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
			default:
				gctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			}
			return
		}

		gctx.JSON(http.StatusOK, gin.H{
			"limite": limit,
			"saldo":  balance,
		})
	})

	// GET /clientes/[id]/extrato
	r.GET("/clientes/:id/extrato", func(gctx *gin.Context) {
		resume, err := getResume(ctx, gctx.Param("id"))
		if err != nil {
			gctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
			return
		}

		resp := fmt.Sprintf(`{
	"saldo": {
		"total": %d,
		"data_extrato": "%s",
		"limite": %d
	},"ultimas_transacoes": [`, resume.Balance, time.Now().Format(time.RFC3339), resume.Limit)
		for i, t := range resume.Transactions {
			if i > 0 {
				resp += ","
			}
			t := fmt.Sprintf(`{
		"valor": %d,
		"tipo": "%s",
		"descricao": "%s",
		"realizada_em": "%s"
	}`, t.Value, t.Type, t.Description, t.Date)
			resp += t
		}
		resp += "]}"
		gctx.Header("Content-Type", "application/json")
		gctx.String(http.StatusOK, resp)
	})

	fmt.Print("Server running on port 9999\n")

	r.Run(":9999")
}
