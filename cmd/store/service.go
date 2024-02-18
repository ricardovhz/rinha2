package main

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ricardovhz/rinha2/db"
	"github.com/ricardovhz/rinha2/model"
	"github.com/ricardovhz/rinha2/repository"
)

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
	s.wg.Add(1)

	// inicia serviço de flush
	go func(ca chan *saveContext) {
		defer s.wg.Done()
		for t := range ca {
			s.buf = append(s.buf, t)
			if len(s.buf) == 100 {
				s.flush()
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
