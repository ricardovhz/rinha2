package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

var debug = os.Getenv("DEBUG") == "true"

var defaultLogFormatter = func(param gin.LogFormatterParams) string {
	var statusColor, methodColor, resetColor string
	if param.IsOutputColor() {
		statusColor = param.StatusCodeColor()
		methodColor = param.MethodColor()
		resetColor = param.ResetColor()
	}

	if param.Latency > time.Minute {
		param.Latency = param.Latency.Truncate(time.Second)
	}
	return fmt.Sprintf("[GIN] %v |%s %3d %s| %13v | %15s |%s %-7s %s %#v\n%s",
		param.TimeStamp.Format("2006/01/02 - 15:04:05"),
		statusColor, param.StatusCode, resetColor,
		param.Latency,
		param.ClientIP,
		methodColor, param.Method, resetColor,
		param.Path,
		param.ErrorMessage,
	)
}

func defaultLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Log only when path is not being skipped
		param := gin.LogFormatterParams{
			Request: c.Request,
			Keys:    c.Keys,
		}

		// Stop timer
		param.TimeStamp = time.Now()
		param.Latency = param.TimeStamp.Sub(start)

		param.ClientIP = c.ClientIP()
		param.Method = c.Request.Method
		param.StatusCode = c.Writer.Status()
		param.ErrorMessage = c.Errors.ByType(gin.ErrorTypePrivate).String()

		param.BodySize = c.Writer.Size()

		if raw != "" {
			path = path + "?" + raw
		}

		param.Path = path

		if debug || param.Latency.Milliseconds() > int64(500) {

			// filtrando requests longas
			fmt.Fprint(os.Stdout, defaultLogFormatter(param))
		}
	}
}

func main() {
	fmt.Printf("GOMAXPROCS is %d\n", runtime.GOMAXPROCS(0))
	var logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		// Level: slog.LevelDebug,
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	storeRepo := repository.NewTcpRepository(os.Getenv("STORE_HOST"))
	repo = storeRepo

	// warming up
	var err error
	for {
		_, err = repo.GetResume(ctx, "1")
		if err == nil {
			slog.Info("Warming up", "client", "1")
			break
		}
	}

	slog.Info("Warming up")
	repo.GetResume(ctx, "2")
	repo.GetResume(ctx, "3")
	repo.GetResume(ctx, "4")
	repo.GetResume(ctx, "5")

	gin.DisableConsoleColor()

	r := gin.New()
	r.Use(defaultLogger())

	p := sync.Pool{
		New: func() any {
			return &model.Transaction{}
		},
	}

	// POST /clientes/[id]/transacoes
	r.POST("/clientes/:id/transacoes", func(gctx *gin.Context) {
		var (
			t  *model.Transaction = p.Get().(*model.Transaction)
			t1 time.Time
			t2 int64
		)
		defer p.Put(t)
		id := gctx.Param("id")

		// reseting
		t.Type = ""
		t.Value = 0
		t.Description = ""
		err := gctx.ShouldBindJSON(t)
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

		t.Timestamp = time.Now().UnixMilli()

		t1 = time.Now()
		limit, balance, err := saveTransaction(ctx, id, t)
		t2 = time.Since(t1).Milliseconds()
		if (t2) > 100 {
			slog.Info("Transaction saved", "id", id, "limit", limit, "balance", balance, "error", err, "duration", t2)
		}
		if err != nil {
			switch err {
			case repository.ErrLimitExceeded:
				gctx.JSON(http.StatusUnprocessableEntity, gin.H{"message": err.Error()})
			case repository.ErrClientNotInitialized:
				gctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
			default:
				gctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			}
			return
		}

		gctx.Data(http.StatusOK, "application/json", []byte(fmt.Sprintf(`{"limite":%d,"saldo":%d}`, limit, balance)))
	})

	resumePool := sync.Pool{
		New: func() any {
			return gin.H{
				"saldo":              gin.H{},
				"ultimas_transacoes": []model.Transaction{},
			}
		},
	}

	// GET /clientes/[id]/extrato
	r.GET("/clientes/:id/extrato", func(gctx *gin.Context) {
		resume, err := getResume(ctx, gctx.Param("id"))
		if err != nil {
			gctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
			return
		}
		for _, t := range resume.Transactions {
			t.Date = time.UnixMilli(t.Timestamp).Format(time.RFC3339Nano)
		}
		h := resumePool.Get().(gin.H)
		defer resumePool.Put(h)
		hs := h["saldo"].(gin.H)
		hs["total"] = resume.Balance
		hs["data_extrato"] = time.Now().Format(time.RFC3339)
		hs["limite"] = resume.Limit
		h["ultimas_transacoes"] = resume.Transactions
		gctx.JSON(200, h)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "9999"
	}
	fmt.Printf("Server running on port %s\n", port)

	r.Run(":" + port)
}
