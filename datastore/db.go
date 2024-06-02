package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	outFileNamePrefix = "segment-"
	maxSegmentSize    = 1024 * 1024 * 10 // 10 MB for example
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type writeRequest struct {
	key   string
	value string
	resp  chan error
}

type Segment struct {
	file   *os.File
	path   string
	offset int64
	index  hashIndex
	mu     sync.Mutex
}

type Db struct {
	segments       []*Segment
	currentSegment *Segment
	dir            string
	writeCh        chan writeRequest
	wg             sync.WaitGroup
	mu             sync.Mutex
	closed         bool
}

func NewDb(dir string) (*Db, error) {
	db := &Db{
		dir:     dir,
		writeCh: make(chan writeRequest),
	}

	err := db.loadSegments()
	if err != nil {
		return nil, err
	}

	err = db.openCurrentSegment()
	if err != nil {
		return nil, err
	}

	db.wg.Add(1)
	go db.writeWorker()

	return db, nil
}

func (db *Db) loadSegments() error {
	files, err := filepath.Glob(filepath.Join(db.dir, outFileNamePrefix+"*"))
	if err != nil {
		return err
	}

	for _, file := range files {
		segment, err := db.loadSegment(file)
		if err != nil {
			return err
		}
		db.segments = append(db.segments, segment)
	}

	return nil
}

func (db *Db) loadSegment(path string) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0o600)
	if err != nil {
		return nil, err
	}

	segment := &Segment{
		file:  file,
		path:  path,
		index: make(hashIndex),
	}

	err = segment.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	return segment, nil
}

func (db *Db) openCurrentSegment() error {
	path := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileNamePrefix, len(db.segments)))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	db.currentSegment = &Segment{
		file:  file,
		path:  path,
		index: make(hashIndex),
	}
	db.segments = append(db.segments, db.currentSegment)

	return nil
}

const bufSize = 8192

func (s *Segment) recover() error {
	input, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer input.Close()

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			s.index[e.key] = s.offset
			s.offset += int64(n)
		} else if err == io.EOF {
			break
		} else {
			return err
		}
	}
	return nil
}

func (db *Db) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	close(db.writeCh)
	db.mu.Unlock()

	db.wg.Wait()

	for _, segment := range db.segments {
		err := segment.file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for i := len(db.segments) - 1; i >= 0; i-- {
		segment := db.segments[i]
		segment.mu.Lock()
		position, ok := segment.index[key]
		segment.mu.Unlock()
		if ok {
			return segment.readFromPosition(position)
		}
	}

	return "", ErrNotFound
}

func (s *Segment) readFromPosition(position int64) (string, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	req := writeRequest{
		key:   key,
		value: value,
		resp:  make(chan error, 1), // Ensure the channel is buffered to prevent deadlock
	}
	db.writeCh <- req
	return <-req.resp
}

func (db *Db) put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	e := entry{
		key:   key,
		value: value,
	}
	data := e.Encode()
	n, err := db.currentSegment.file.Write(data)
	if err != nil {
		return err
	}

	db.currentSegment.mu.Lock()
	db.currentSegment.index[key] = db.currentSegment.offset
	db.currentSegment.offset += int64(n)
	db.currentSegment.mu.Unlock()

	if db.currentSegment.offset >= maxSegmentSize {
		err = db.rotateSegment()
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Db) rotateSegment() error {
	err := db.currentSegment.file.Close()
	if err != nil {
		return err
	}

	return db.openCurrentSegment()
}

func (db *Db) writeWorker() {
	defer db.wg.Done()
	for req := range db.writeCh {
		err := db.put(req.key, req.value)
		req.resp <- err
		close(req.resp)
	}
}
