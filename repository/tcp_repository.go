package repository

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/ricardovhz/rinha2/db"
	"github.com/ricardovhz/rinha2/model"
)

type ByteHolder struct {
	b []byte
}

func NewByteHolder(s int) *ByteHolder {
	return &ByteHolder{
		b: make([]byte, s),
	}
}

type tcpRepository struct {
	addr             string
	pool             *ConnPool
	requestBytePool  *sync.Pool
	responseBytePool *sync.Pool
}

func (t *tcpRepository) validateResponse(resp []byte) error {
	if resp[0] == 'e' {
		if resp[1] == 'n' {
			return ErrClientNotInitialized
		}
		if resp[1] == 'l' {
			return ErrLimitExceeded
		}
	}
	return nil
}

func (t *tcpRepository) toIntLittleEndian(resp []byte) int {
	return int(resp[0]) | int(resp[1])<<8 | int(resp[2])<<16 | int(resp[3])<<24
}

func (t *tcpRepository) GetLimitAndBalance(ctx context.Context, id string) (int, int, error) {
	return -1, -1, nil
}

func (t *tcpRepository) SaveTransaction(ctx context.Context, id string, tr *model.Transaction) (int, int, error) {
	bh := t.requestBytePool.Get().(*ByteHolder)
	defer t.requestBytePool.Put(bh)
	msg := bh.b
	msg[0] = '1'
	r := db.ToRecord(id, tr)
	copy(msg[1:], r[:])

	d, err := t.pool.Get()
	if err != nil {
		slog.Info("error dialing", "err", ErrClientNotInitialized)
		return -1, -1, ErrClientNotInitialized
	}
	defer t.pool.Put(d)

	_, err = d.Write(msg)
	if err != nil {
		return -1, -1, err
	}

	bhr := t.responseBytePool.Get().(*ByteHolder)
	defer t.responseBytePool.Put(bhr)
	resp := bhr.b
	i, err := d.Read(resp)
	if err != nil {
		return -1, -1, err
	}

	if err = t.validateResponse(resp); err != nil {
		return -1, -1, err
	} else if i != 9 {
		slog.Error("invalid response: " + string(resp))
		return -1, -1, nil
	}

	lim := t.toIntLittleEndian(resp[1:5])
	bal := int(int32(t.toIntLittleEndian(resp[5:9])))

	return lim, bal, nil
}

func (t *tcpRepository) GetResume(ctx context.Context, id string) (*model.Resume, error) {
	msg := [2]byte{'0', id[0]}

	d, err := t.pool.Get()
	if err != nil {
		slog.Info("error dialing", "err", ErrClientNotInitialized)
		return nil, ErrClientNotInitialized
	}
	defer t.pool.Put(d)

	_, err = d.Write(msg[:])
	if err != nil {
		return nil, err
	}

	resp := make([]byte, 9+db.RecordSize*5)

	i, err := d.Read(resp)
	if err != nil {
		return nil, err
	}

	if err = t.validateResponse(resp); err != nil {
		return nil, err
	} else if i < 9 {
		slog.Error("invalid response: " + string(resp))
		return nil, ErrClientNotInitialized
	}

	lim := t.toIntLittleEndian(resp[1:5])
	bal := int(int32(t.toIntLittleEndian(resp[5:9])))

	trs := make([]*model.Transaction, 0)
	for j := 9; j < i; j += db.RecordSize {
		r, err := db.ReadRecord(bytes.NewReader(resp[j : j+db.RecordSize]))
		if err != nil {
			panic(err)
		}
		_, tr := db.ToTransaction(r)
		trs = append(trs, tr)
	}

	return &model.Resume{
		Limit:        lim,
		Balance:      bal,
		Transactions: trs,
	}, nil
}

func (t *tcpRepository) ShutDown() {

	// TODO fechar as conexões do pool
}

func NewTcpRepository(addr string) Repository {
	tcpPoolSize, _ := strconv.Atoi(os.Getenv("TCP_POOL_SIZE"))
	if tcpPoolSize <= 0 {
		tcpPoolSize = 10
	}
	return &tcpRepository{
		addr: addr,
		pool: NewPool(tcpPoolSize, func() (net.Conn, error) {
			return net.Dial("tcp", addr)
		}),
		requestBytePool: &sync.Pool{
			New: func() any {
				return NewByteHolder(25)
			},
		},
		responseBytePool: &sync.Pool{
			New: func() any {
				return NewByteHolder(9)
			},
		},
	}
}
