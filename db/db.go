package db

import (
	"github.com/ricardovhz/rinha2/model"
	"github.com/segmentio/ksuid"
)

type DB struct {
	wf writerFactory
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

func NewDB(wf writerFactory) *DB {
	return &DB{wf: wf}
}
