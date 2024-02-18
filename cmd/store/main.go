package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	_ "net/http/pprof"

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

type storeService struct {
	db *db.DB

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
	// tx, err := s.db.BeginTx(s.ctx, nil)
	// if err != nil {
	// 	slog.Error("error flushing", "err", err)
	// 	return
	// }
	// sql := "INSERT INTO transactions (client_id, t, amount, description, date) VALUES "
	// for _, t := range s.buf {
	// 	sql += fmt.Sprintf("('%s', '%s', '%d', '%s', datetime()),", t.id, t.transaction.Type, t.transaction.Value, t.transaction.Description)
	// }
	// _, err = tx.ExecContext(s.ctx, sql[:len(sql)-1])
	// if err != nil {
	// 	slog.Error("error execing flush", "err", err)
	// 	tx.Rollback()
	// 	return
	// }
	// tx.Commit()

	tr := make([]*model.Transaction, len(s.buf))
	id := ""
	for i, sc := range s.buf {
		if i == 0 {
			id = sc.id
		}
		tr[i] = sc.transaction
	}
	err := s.db.Write(id, tr)
	if err != nil {
		slog.Error("error writing to db", "err", err, "id", id)
	}
}

func (s *storeService) start() {
	// _, err := s.db.Exec("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY, client_id TEXT, t TEXT, amount REAL, description TEXT, date TEXT)")
	// if err != nil {
	// 	panic(err)
	// }
	// _, err = s.db.Exec("CREATE INDEX IF NOT EXISTS idx_client_id ON transactions (client_id)")
	// if err != nil {
	// 	panic(err)
	// }

	s.wg.Add(1)
	go func(ca chan *saveContext) {
		defer s.wg.Done()
		for t := range ca {
			s.buf = append(s.buf, t)
			if len(s.buf) == 100 {
				// t1 := time.Now().UnixMilli()
				s.flush()
				// t2 := time.Now().UnixMilli()
				//slog.Debug("flushed", "len", len(s.buf), "time", t2-t1)

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

func (s *storeService) Save(ctx context.Context, r db.Record) (int32, int32, error) {
	// validate
	id, tr := db.ToTransaction(r)
	var clientLock *sync.Mutex
	if cl, ok := s.l[id]; !ok {
		return -1, -1, repository.ErrClientNotInitialized
	} else {
		clientLock = cl
	}

	t1 := time.Now()
	defer func() {
		t2 := time.Since(t1).Milliseconds()
		if t2 > 100 {
			slog.Info("Transaction saved", "id", id, "value", tr.Value, "type", tr.Type, "duration", t2)
		}
	}()

	clientLock.Lock()

	infos := s.clientInfos[id]

	lim := infos.limit
	bal := infos.balance

	nowTime := time.Now().Format(time.RFC3339Nano)
	tr.Date = nowTime

	val := tr.Value
	if tr.Type == "d" {
		val *= -1
	}

	for {
		if tr.Type == "d" && (bal+int32(val)) < lim*-1 {
			clientLock.Unlock()
			return -1, -1, repository.ErrLimitExceeded
		}

		if bal != infos.addBalance(int32(val)) {
			infos := s.clientInfos[id]
			bal = infos.balance
			continue
		} else {
			infos.addTransaction(tr)
			break
		}
	}

	clientLock.Unlock()

	s.c <- &saveContext{
		id:          id,
		transaction: tr,
	}
	return lim, bal + int32(tr.Value), nil
}

func (s *storeService) GetExtract(ctx context.Context, id string) (int32, int32, []*model.Transaction, error) {
	var infos *clientInfo
	if l, ok := s.clientInfos[id]; !ok {
		return -1, -1, nil, repository.ErrClientNotInitialized
	} else {
		infos = l
	}
	lim := infos.limit
	bal := infos.balance

	// so := &pb.GetExtractResponse{
	// 	Limit:        lim,
	// 	Balance:      bal,
	// 	Transactions: make([]*pb.Transaction, 0),
	// }

	res := make([]*model.Transaction, 0)

	lt := infos.lastTransactions
	for _, t := range lt {
		if t != nil {
			res = append(res, t)
		}
	}
	sort.SliceStable(res, func(i, j int) bool {
		return res[j].Date < res[i].Date
	})

	return lim, bal, res, nil
}

func (s *storeService) InitializeClient(id string, limit int32, balance int32) {
	s.l[id] = &sync.Mutex{}
	s.clientInfos[id] = &clientInfo{
		limit:            limit,
		balance:          balance,
		counter:          0,
		lastTransactions: make([]*model.Transaction, 5),
	}
}

func NewStoreService(ctx context.Context, db *db.DB) *storeService {
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

	// os.Remove("store.db")

	// db, err := sql.Open("sqlite3", "store.db")
	// if err != nil {
	// 	panic(err)
	// }
	// defer db.Close()

	dba := db.NewDB(db.NewFileWriterFactory())

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	serv := NewStoreService(ctx, dba)
	// defer serv.Close()

	serv.InitializeClient("1", 100000, 0)
	serv.InitializeClient("2", 80000, 0)
	serv.InitializeClient("3", 1000000, 0)
	serv.InitializeClient("4", 10000000, 0)
	serv.InitializeClient("5", 500000, 0)

	// pb.RegisterStoreServiceServer(srv, serv)

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

	// srv.Serve(lis)

	for {
		conn, err := lis.Accept()
		if err != nil {
			slog.Error("accept error", "err", err)
			continue
		}
		slog.Info("accepted", "conn", conn.RemoteAddr())
		go func() {
			defer conn.Close()

			for {
				b := make([]byte, 25)
				i, err := conn.Read(b)
				if err != nil {
					// slog.Debug("error reading", "err", err, "i", i, "b", b)
					return
				}
				switch i {
				case 2:
					// extract
					id := string(b[1])
					lim, bal, tr, err := serv.GetExtract(ctx, id)
					if err != nil {
						slog.Error("error getting extract", "err", err, "id", id)
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
						slog.Error("error saving transaction", "err", err)
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
