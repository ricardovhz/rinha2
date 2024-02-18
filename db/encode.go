package db

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/ricardovhz/rinha2/model"
)

// estrutura do registro

// client_id (byte)
//
//	  ^              timestamp (int64)        value (int32)         description (string)
//	  |       |---------------------------|  |------------|   |-----------------------------|
//	+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
//	| 1 | c | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | x |x10
//	+---+-+-+---+---+---+---+---+---+---+---+---+---+---+---+---+
//		  |
//	      v
//		type (byte)
const RecordSize = 24

type Record [RecordSize]byte

type RegWriter interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
}

func ToRecord(id string, t *model.Transaction) Record {
	r := Record{}
	r[0] = id[0]
	r[1] = byte(t.Type[0])
	binary.LittleEndian.PutUint64(r[2:10], uint64(t.Timestamp))
	binary.LittleEndian.PutUint32(r[10:14], uint32(t.Value))
	copy(r[14:], []byte(t.Description))
	return r
}

func WriteToRecord(id string, t *model.Transaction, w *Record) {
	w[0] = id[0]
	w[1] = byte(t.Type[0])
	binary.LittleEndian.PutUint64(w[2:10], uint64(t.Timestamp))
	binary.LittleEndian.PutUint32(w[10:14], uint32(t.Value))
	copy(w[14:], []byte(t.Description)[0:])
}

func ReadRecord(r io.Reader) (Record, error) {
	var rec Record
	_, err := r.Read(rec[:])
	return rec, err
}

func ToTransaction(r Record) (string, *model.Transaction) {
	timestamp := int64(binary.LittleEndian.Uint64(r[2:10]))
	return string(r[0]), &model.Transaction{
		Timestamp:   timestamp,
		Type:        string(r[1]),
		Value:       int(int32(binary.LittleEndian.Uint32(r[10:14]))),
		Description: string(bytes.Trim(r[14:], "\x00")),
	}
}
