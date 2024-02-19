package db_test

import (
	"log"
	"testing"

	"github.com/ricardovhz/rinha2/db"
)

func TestReader(t *testing.T) {
	frr := db.NewFileRegReader(".")

	records, err := frr.ReadLast("1", 5)
	if err != nil {
		log.Printf("error reading records: %v", err)
		t.Fail()
	}
	log.Printf("records: %v", records)
	for i, r := range records {
		id, tr := db.ToTransaction(r)
		if err != nil {
			t.Fail()
		}
		log.Printf("%d record client %s: %v", i, id, tr)
	}

	records, err = frr.ReadLast("2", 5)
	if err != nil {
		log.Printf("error reading records: %v", err)
		t.Fail()
	}
	log.Printf("records: %v", records)
	for i, r := range records {
		id, tr := db.ToTransaction(r)
		if err != nil {
			t.Fail()
		}
		log.Printf("%d record client %s: %v", i, id, tr)
	}
}

func TestBalance(t *testing.T) {
	frr := db.NewFileRegReader(".")

	balance, err := frr.GetBalance("1")
	if err != nil {
		log.Printf("error reading balance: %v", err)
		t.Fail()
	}
	log.Printf("balance 1: %d", balance)

	balance, err = frr.GetBalance("2")
	if err != nil {
		log.Printf("error reading balance: %v", err)
		t.Fail()
	}
	log.Printf("balance 2: %d", balance)
}
