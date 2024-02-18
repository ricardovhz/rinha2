package db

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ricardovhz/rinha2/model"
	"github.com/segmentio/ksuid"
)

/*
estrutura do registro

client_id (byte)

	^                 timestamp (int64)        value (int32)         description (string)
	|         |----------------------------|  |-----------|   |-----------------------------|

	+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
	| 1 | c | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | x |x10
	+---+-+-+---+---+---+---+---+---+---+---+---+---+---+---+---+

		  |
	      v
		type (byte)
*/
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

func WriteRecord(id string, t *model.Transaction, w *Record) error {
	w[0] = id[0]
	w[1] = byte(t.Type[0])
	binary.LittleEndian.PutUint64(w[2:10], uint64(t.Timestamp))
	binary.LittleEndian.PutUint32(w[10:14], uint32(t.Value))
	copy(w[14:], []byte(t.Description)[0:])
	// w.WriteByte(id[0])
	// w.WriteByte(t.Type[0])

	// timestamp
	// w.WriteByte(byte(t.Timestamp) & 0xFF)
	// w.WriteByte(byte(t.Timestamp>>8) & 0xFF)
	// w.WriteByte(byte(t.Timestamp>>16) & 0xFF)
	// w.WriteByte(byte(t.Timestamp>>24) & 0xFF)
	// w.WriteByte(byte(t.Timestamp>>32) & 0xFF)
	// w.WriteByte(byte(t.Timestamp>>40) & 0xFF)
	// w.WriteByte(byte(t.Timestamp>>48) & 0xFF)
	// w.WriteByte(byte(t.Timestamp>>56) & 0xFF)

	// value
	// i32Value := int32(t.Value)
	// w.WriteByte(byte(i32Value) & 0xFF)
	// w.WriteByte(byte(i32Value>>8) & 0xFF)
	// w.WriteByte(byte(i32Value>>16) & 0xFF)
	// w.WriteByte(byte(i32Value>>24) & 0xFF)

	// for i := 0; i < 10; i++ {
	// 	if i < len(t.Description) {
	// 		w.WriteByte(t.Description[i])
	// 	} else {
	// 		w.WriteByte(0)
	// 	}
	// }
	return nil
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

type CloseableRegWriter interface {
	RegWriter
	io.Closer
}

type flushableRegWriter struct {
	b *bufio.Writer
	w io.WriteCloser
}

func (frw *flushableRegWriter) Write(p []byte) (int, error) {
	return frw.b.Write(p)
}
func (frw *flushableRegWriter) WriteByte(b byte) error {
	return frw.b.WriteByte(b)
}
func (frw *flushableRegWriter) Close() error {
	frw.b.Flush()
	return frw.w.Close()
}

type writerFactory interface {
	NewWriter(id, chunkId string, transactionLen int) (CloseableRegWriter, error)
}

type fileWriterFactory struct {
	path string
}

func (fwf *fileWriterFactory) NewWriter(id, chunkId string, transactionLen int) (CloseableRegWriter, error) {
	dir := fwf.path + id
	os.Mkdir(dir, os.ModeDir|0755)
	filename := fmt.Sprintf("%s/%s/%s", fwf.path, id, chunkId)
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	lastPath := filepath.Join(dir, "LAST")
	os.Remove(lastPath)
	err = os.Symlink(chunkId, lastPath)
	if err != nil {
		return nil, err
	}
	return &flushableRegWriter{b: bufio.NewWriterSize(f, transactionLen), w: f}, nil
}

var pathPrefix = os.Getenv("PATH_PREFIX")

func NewFileWriterFactory() writerFactory {
	return &fileWriterFactory{
		path: pathPrefix,
	}
}

func NewFileWriterFactoryFromPath(p string) writerFactory {
	return &fileWriterFactory{
		path: p,
	}
}

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
		WriteRecord(id, tr, &r)
		// if err != nil {
		// 	return err
		// }
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
