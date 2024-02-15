package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type saveContext struct {
	id          string
	transaction *model.Transaction
}

type clientInfo struct {
	limit            int32
	balance          int32
	counter          int32
	lastTransactions []*pb.Transaction
}

func (c *clientInfo) addBalance(b int32) {
	c.balance += b
}

func (c *clientInfo) addTransaction(t *pb.Transaction) {
	n := atomic.AddInt32(&c.counter, 1)
	c.lastTransactions[n%5] = t
}

type storeService struct {
	pb.StoreServiceServer
	db *sql.DB

	l           map[string]*sync.Mutex
	clientInfos map[string]*clientInfo

	c    chan *saveContext
	wg   *sync.WaitGroup
	ctx  context.Context
	once *sync.Once
	buf  []*saveContext
}

func (s *storeService) flush() {
	// slog.Debug("flushing", "len", len(s.buf))
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		slog.Error("error flushing", "err", err)
		return
	}
	sql := "INSERT INTO transactions (client_id, t, amount, description, date) VALUES "
	for _, t := range s.buf {
		sql += fmt.Sprintf("('%s', '%s', '%d', '%s', datetime()),", t.id, t.transaction.Type, t.transaction.Value, t.transaction.Description)
	}
	_, err = tx.ExecContext(s.ctx, sql[:len(sql)-1])
	if err != nil {
		slog.Error("error execing flush", "err", err)
		tx.Rollback()
		return
	}
	tx.Commit()
}

func (s *storeService) start() {
	_, err := s.db.Exec("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, client_id TEXT, t TEXT, amount REAL, description TEXT, date TEXT)")
	if err != nil {
		panic(err)
	}
	_, err = s.db.Exec("CREATE INDEX IF NOT EXISTS idx_client_id ON transactions (client_id)")
	if err != nil {
		panic(err)
	}

	s.wg.Add(1)
	go func(ca chan *saveContext) {
		defer s.wg.Done()
		for t := range ca {
			s.buf = append(s.buf, t)
			if len(s.buf) == 100 {
				t1 := time.Now().UnixMilli()
				s.flush()
				t2 := time.Now().UnixMilli()
				slog.Debug("flushed", "len", len(s.buf), "time", t2-t1)

				s.buf = make([]*saveContext, 0)
			}
		}
		slog.Debug("closing")
		if len(s.buf) > 0 {
			s.flush()
		}
	}(s.c)
}

func (s *storeService) Close() {
	close(s.c)
	s.wg.Wait()
}

func (s *storeService) Save(ctx context.Context, sr *pb.SaveRequest) (*pb.SaveResponse, error) {
	// validate
	var clientLock *sync.Mutex
	if cl, ok := s.l[sr.Transaction.ClientId]; !ok {
		return &pb.SaveResponse{
			Error:   true,
			Message: "client not initialized",
		}, nil
	} else {
		clientLock = cl
	}
	clientLock.Lock()
	defer clientLock.Unlock()

	infos := s.clientInfos[sr.Transaction.ClientId]

	lim := infos.limit
	bal := infos.balance

	if sr.Transaction.Type == "d" {
		sr.Transaction.Value *= -1
		if (bal + int32(sr.Transaction.Value)) < lim*-1 {
			return &pb.SaveResponse{
				Error:   true,
				Message: "limit exceeded",
			}, nil
		}
	}
	nowTime := time.Now().Format(time.RFC3339)
	sr.Transaction.Date = nowTime
	infos.addBalance(int32(sr.Transaction.Value))
	infos.addTransaction(sr.Transaction)

	t := &model.Transaction{
		Date:        nowTime,
		Value:       int(sr.Transaction.Value),
		Type:        sr.Transaction.Type,
		Description: sr.Transaction.Description,
	}
	s.c <- &saveContext{
		id:          sr.Transaction.ClientId,
		transaction: t,
	}
	return &pb.SaveResponse{
		LimitAndBalance: &pb.LimitAndBalance{
			Limit:   lim,
			Balance: bal + int32(sr.Transaction.Value),
		},
	}, nil
}

func (s *storeService) GetExtract(ctx context.Context, req *pb.GetExtractRequest) (*pb.GetExtractResponse, error) {
	var infos *clientInfo
	if l, ok := s.clientInfos[req.ClientId]; !ok {
		return nil, fmt.Errorf("client not initialized: %s", req.ClientId)
	} else {
		infos = l
	}
	lim := infos.limit
	bal := infos.balance

	so := &pb.GetExtractResponse{
		LimitAndBalance: &pb.LimitAndBalance{
			Limit:   lim,
			Balance: bal,
		},
		Transactions: make([]*pb.Transaction, 0),
	}

	lt := infos.lastTransactions
	for _, t := range lt {
		if t != nil {
			so.Transactions = append(so.Transactions, t)
		}
	}
	sort.SliceStable(so.Transactions, func(i, j int) bool {
		return so.Transactions[j].Date < so.Transactions[i].Date
	})

	return so, nil
}

func (s *storeService) InitializeClient(id string, limit int32, balance int32) {
	s.l[id] = &sync.Mutex{}
	s.clientInfos[id] = &clientInfo{
		limit:            limit,
		balance:          balance,
		counter:          0,
		lastTransactions: make([]*pb.Transaction, 5),
	}
}

func NewStoreService(ctx context.Context, db *sql.DB) *storeService {
	c := make(chan *saveContext)
	s := &storeService{
		db: db,

		l:           make(map[string]*sync.Mutex),
		clientInfos: make(map[string]*clientInfo),

		c:    c,
		wg:   &sync.WaitGroup{},
		ctx:  ctx,
		once: &sync.Once{},
		buf:  make([]*saveContext, 0),
	}
	s.start()
	return s
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	os.Remove("store.db")

	db, err := sql.Open("sqlite3", "store.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	serv := NewStoreService(ctx, db)
	defer serv.Close()

	serv.InitializeClient("1", 100000, 0)
	serv.InitializeClient("2", 80000, 0)
	serv.InitializeClient("3", 1000000, 0)
	serv.InitializeClient("4", 10000000, 0)
	serv.InitializeClient("5", 500000, 0)

	pb.RegisterStoreServiceServer(srv, serv)

	lis, err := net.Listen("tcp", "localhost:5001")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Listening on tcp port %d\n", 5001)
	srv.Serve(lis)
}
