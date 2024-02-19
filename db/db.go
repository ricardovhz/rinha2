package db

import (
	"github.com/ricardovhz/rinha2/model"
	"github.com/segmentio/ksuid"
)

type DB struct {
	wf writerFactory
	r  RegReader
}

func (db *DB) Write(id string, t []*model.Transaction) error {
	chunkId := ksuid.New().String()
	w, err := db.wf.NewWriter(id, chunkId, len(t))
	if err != nil {
		return err
	}
	defer w.Close()
	r := Record{}
	for _, tr := range t {
		WriteToRecord(id, tr, &r)
		_, err = w.Write(r[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) ReadLast(id string) ([]*model.Transaction, error) {
	records, err := db.r.ReadLast(id, 5)
	if err != nil {
		return nil, err
	}
	tr := make([]*model.Transaction, len(records))
	for i, r := range records {
		_, tr[i] = ToTransaction(r)
	}
	return tr, nil
}

func (db *DB) ReadBalance(id string) (int32, error) {
	bal, err := db.r.GetBalance(id)
	if err != nil {
		return -1, err
	}
	return bal, nil
}

func NewDB(wf writerFactory, r RegReader) *DB {
	return &DB{
		wf: wf,
		r:  r,
	}
}
