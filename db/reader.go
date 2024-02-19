package db

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type RegReader interface {
	ReadLast(id string, n int) ([]Record, error)
	GetBalance(id string) (int32, error)
}

type fileRegReader struct {
	path string
}

func (frr *fileRegReader) ReadLast(id string, n int) ([]Record, error) {
	dbf, err := os.Readlink(filepath.Join(frr.path, id, "LAST"))
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(frr.path, id, dbf))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, err = f.Seek(int64(-n*RecordSize), io.SeekEnd)
	if err != nil {
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
	}
	b := make([]Record, 0)

	for i := 0; i < n; i++ {
		r := Record{}
		_, err = f.Read(r[:])
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}
		b = append(b, r)
	}

	return b, nil
}

func (frr *fileRegReader) GetBalance(id string) (int32, error) {
	d, err := os.ReadDir(filepath.Join(frr.path, id))
	if err != nil {
		return -1, err
	}

	c := make(chan int32)

	wg := sync.WaitGroup{}

	go func() {
		wg.Wait()
		close(c)
	}()

	for _, de := range d {
		if de.Name() == "LAST" {
			continue
		}
		wg.Add(1)

		go func(en os.DirEntry, ch chan<- int32) {
			defer wg.Done()
			f, err := os.Open(filepath.Join(frr.path, id, en.Name()))
			if err != nil {
				return
			}
			defer f.Close()

			// posiciona no primeiro registro de valor (1 (id) + 1 (type) + 8 (timestamp))
			_, err = f.Seek(1, io.SeekStart)
			if err != nil {
				return
			}

			for {
				bn := make([]byte, 13)
				_, err = f.Read(bn)
				if err != nil {
					return
				}

				bal := int32(int(bn[9]) | int(bn[10])<<8 | int(bn[11])<<16 | int(bn[12])<<24)
				if bn[0] == 'd' {
					bal *= -1
				}
				c <- bal

				// posiciona no valor do proximo registro (10 (descr) + 1 (id) + 1 (type) + 8 (timestamp))
				_, err = f.Seek(11, io.SeekCurrent)
				if err != nil {
					return
				}
			}

		}(de, c)
	}

	var total int32
	for e := range c {
		total += e
	}

	return total, nil
}

func NewFileRegReader(path string) RegReader {
	return &fileRegReader{path: path}
}
