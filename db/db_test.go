package db_test

import (
	"log"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/ricardovhz/rinha2/db"
	"github.com/ricardovhz/rinha2/model"
	"github.com/stretchr/testify/require"
)

func TestToRecord(t *testing.T) {
	tr := model.Transaction{
		Timestamp:   time.Now().UnixMilli(),
		Value:       100,
		Type:        "c",
		Description: "test",
	}
	r := db.ToRecord("1", &tr)
	log.Printf("%v", r)
}

func TestWriteToRecord(t *testing.T) {
	tr := model.Transaction{
		Timestamp:   time.Now().UnixMilli(),
		Value:       100,
		Type:        "c",
		Description: "testasdasdasdasdasd",
	}
	r := db.Record{}
	// buf := bytes.NewBuffer(r[:])
	// buf.Reset()
	err := db.WriteRecord("1", &tr, &r)
	if err != nil {
		t.Fail()
	}
	log.Printf("%v", r)
}

func TestRead(t *testing.T) {
	r := db.Record{49, 99, 150, 115, 216, 182, 141, 1, 0, 0, 100, 0, 0, 0, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0}
	id, tr := db.ToTransaction(r)
	require.Equal(t, "1", id)
	require.Equal(t, "c", tr.Type)
	require.Equal(t, int64(1708169655190), tr.Timestamp)
	require.Equal(t, 100, tr.Value)
	require.Equal(t, "test", tr.Description)

	r = db.Record{49, 99, 33, 221, 97, 186, 141, 1, 0, 0, 156, 255, 255, 255, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0}
	id, tr = db.ToTransaction(r)
	require.Equal(t, "1", id)
	require.Equal(t, "c", tr.Type)
	require.Equal(t, int64(1708228992289), tr.Timestamp)
	require.Equal(t, -100, tr.Value)
	require.Equal(t, "test", tr.Description)
}

func BenchmarkRecord(b *testing.B) {
	tr := model.Transaction{
		Timestamp:   time.Now().UnixMilli(),
		Value:       100,
		Type:        "c",
		Description: "test",
	}
	r := db.Record{}
	// buf := bytes.NewBuffer(r[:24])
	// buf.Reset()

	// buff := bufio.NewWriter(buf)

	for i := 0; i < b.N; i++ {
		// db.ToRecord("1", &tr)
		db.WriteRecord("1", &tr, &r)
		// buf.Reset()
	}
}

func BenchmarkRead(b *testing.B) {
	r := db.Record{49, 99, 150, 115, 216, 182, 141, 1, 0, 0, 100, 0, 0, 0, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0}
	for i := 0; i < b.N; i++ {
		db.ToTransaction(r)
	}
}

func TestDB(t *testing.T) {

	os.Remove("cpu.prof")
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatalf("could not create cpu profile: %v", err)
		t.Fail()
	}
	defer f.Close()

	db := db.NewDB(db.NewFileWriterFactory())

	tr := make([]*model.Transaction, 100)
	for i := 0; i < 100; i++ {
		tr[i] = &model.Transaction{
			Timestamp:   time.Now().UnixMilli(),
			Value:       100,
			Type:        "c",
			Description: "test",
		}
	}

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	for i := 0; i < 1000; i++ {
		err = db.Write("1", tr)
		if err != nil {
			log.Printf("%v", err)
			t.Fail()
		}
	}

}

func BenchmarkDB(b *testing.B) {
	db := db.NewDB(db.NewFileWriterFactory())

	tr := make([]*model.Transaction, 100)
	for i := 0; i < 100; i++ {
		tr[i] = &model.Transaction{
			Timestamp:   time.Now().UnixMilli(),
			Value:       100,
			Type:        "c",
			Description: "test",
		}
	}

	// b.StartTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Write("1", tr)
		if err != nil {
			log.Printf("%v", err)
			b.Fail()
		}
	}
	b.StopTimer()
}
