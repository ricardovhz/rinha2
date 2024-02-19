package db

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
)

type CloseableRegWriter interface {
	RegWriter
	io.Closer
}

type writerFactory interface {
	NewWriter(id, chunkId string, transactionLen int) (CloseableRegWriter, error)
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

// fileWriterFactory criar arquivos para escrita
type fileWriterFactory struct {
	path string
}

func (fwf *fileWriterFactory) NewWriter(id, chunkId string, transactionLen int) (CloseableRegWriter, error) {
	dir := filepath.Join(fwf.path, id)
	os.Mkdir(dir, os.ModeDir|0755)
	filename := filepath.Join(dir, chunkId)
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

func NewFileWriterFactoryFromPath(p string) writerFactory {
	return &fileWriterFactory{
		path: p,
	}
}
