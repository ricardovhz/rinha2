package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/ricardovhz/rinha2/db"
	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

type saveContext struct {
	id          string
	transaction *model.Transaction
}

type clientInfo struct {
	limit            int32
	balance          int32
	counter          int32
	lastTransactions []*model.Transaction
}

func (c *clientInfo) addBalance(b int32) int32 {
	bal := c.balance
	c.balance += b
	return bal
}

func (c *clientInfo) addTransaction(t *model.Transaction) {
	n := atomic.AddInt32(&c.counter, 1)
	c.lastTransactions[n%5] = t
}

func main() {
	opt := &slog.HandlerOptions{
		Level: slog.LevelError,
	}
	if os.Getenv("DEBUG") != "" {
		opt.Level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opt))
	slog.SetDefault(logger)

	pathPrefix := os.Getenv("PATH_PREFIX")
	dba := db.NewDB(db.NewFileWriterFactoryFromPath(pathPrefix), db.NewFileRegReader(pathPrefix))

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	serv := NewStoreService(ctx, dba)
	defer serv.Close()

	serv.InitializeClient("1", 100000, 0)
	serv.InitializeClient("2", 80000, 0)
	serv.InitializeClient("3", 1000000, 0)
	serv.InitializeClient("4", 10000000, 0)
	serv.InitializeClient("5", 500000, 0)

	typ := os.Getenv("STORE_CONN_TYPE")
	if typ == "" {
		typ = "tcp"
	}

	if typ == "unix" {
		slog.Debug("removing", "path", os.Getenv("STORE_HOST"))
		os.Remove(os.Getenv("STORE_HOST"))
	}

	lis, err := net.Listen(typ, os.Getenv("STORE_HOST"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Listening on %s\n", os.Getenv("STORE_HOST"))

	running := true

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	conns := make([]net.Conn, 0)

	go func() {
		<-c
		slog.Info("shutting down", "conns", len(conns))
		running = false
		for _, c2 := range conns {
			err = c2.Close()
			if err != nil {
				slog.Error("error closing conn", "err", err)
			}
		}
		lis.Close()
	}()

	for running {
		conn, err := lis.Accept()
		if err != nil {
			slog.Error("accept error", "err", err)
			continue
		}
		slog.Info("accepted", "conn", conn.RemoteAddr())
		conns = append(conns, conn)
		go func() {
			for {
				b := make([]byte, 25)
				i, err := conn.Read(b)
				if err != nil {
					return
				}
				switch i {
				case 2:
					// extract
					id := string(b[1])
					lim, bal, tr, err := serv.GetExtract(ctx, id)
					if err != nil {
						slog.Debug("error getting extract", "err", err, "id", id)
						respErr := [2]byte{'e', ' '}
						if err == repository.ErrClientNotInitialized {
							respErr[1] = 'n'
						} else if err == repository.ErrLimitExceeded {
							respErr[1] = 'l'
						}
						conn.Write(respErr[:])
						continue
					}
					resp := make([]byte, 9+db.RecordSize*len(tr))
					resp[0] = '0'
					binary.LittleEndian.PutUint32(resp[1:], uint32(lim))
					binary.LittleEndian.PutUint32(resp[5:], uint32(bal))
					for j, t := range tr {
						re := db.ToRecord(id, t)
						copy(resp[9+j*db.RecordSize:], re[:])
					}
					conn.Write(resp)
				case 25:
					// save
					r := db.Record(b[1:])
					lim, bal, err := serv.Save(ctx, r)
					if err != nil {
						slog.Debug("error saving transaction", "err", err)
						respErr := [2]byte{'e', ' '}
						if err == repository.ErrClientNotInitialized {
							respErr[1] = 'n'
						} else if err == repository.ErrLimitExceeded {
							respErr[1] = 'l'
						}
						conn.Write(respErr[:])
						continue
					}

					resp := make([]byte, 9)
					resp[0] = '0'
					binary.LittleEndian.PutUint32(resp[1:], uint32(lim))
					binary.LittleEndian.PutUint32(resp[5:], uint32(bal))
					_, err = conn.Write(resp)
					if err != nil {
						panic(err)
					}
				default:
					slog.Error("invalid message", "b", b)
				}
			}
		}()
	}
}
