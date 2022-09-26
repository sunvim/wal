package wal

import (
	"sync"

	"github.com/sunvim/utils/cachem"
)

type Log struct {
	opts      *Option
	writer    IFile
	fistIndex uint64
	lastIndex uint64
}

func Open(path string, opts *Option) (*Log, error) {
	var err error

	l := &Log{}
	l.writer, err = OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	err = l.writer.Check()
	if err != nil {
		return nil, err
	}
	head, _ := l.writer.Header()
	l.fistIndex = head.head
	l.lastIndex = head.tail

	return nil, nil
}

func (l *Log) Close() error {
	return l.writer.Close()
}

var (
	rpool = sync.Pool{
		New: func() interface{} {
			return &Record{}
		},
	}
)

func (l *Log) Write(idx uint64, data []byte) error {
	r, _ := rpool.Get().(*Record)
	defer rpool.Put(r)
	r.index = idx
	r.data = data
	head, err := l.writer.Header()
	if err != nil {
		return err
	}
	head.tail = r.index
	l.writer.WriteAt(head.Marshal(), 0)
	l.writer.Write(r.Marshal())
	return nil
}

func (l *Log) Read(idx uint64) (data []byte, err error) {
	items, err := l.writer.Items()
	if err != nil {
		return nil, err
	}
	var rec *Record
	for _, item := range items {
		if item.index == idx {
			buf := cachem.Malloc(int(item.length))
			l.writer.ReadAt(buf, int64(item.offset))
			rec.Unmarshal(buf)
			cachem.Free(buf)
			if rec.index != idx {
				continue
			}
			return rec.data, nil
		}
	}
	return nil, ErrNotFound
}

func (l *Log) ReadBatch(idxes ...uint64) (map[uint64][]byte, error) {
	if len(idxes) == 0 {
		return nil, nil
	}
	m := make(map[uint64][]byte, len(idxes))
	items, err := l.writer.Items()
	if err != nil {
		return nil, err
	}
	var rec *Record
	for _, item := range items {
		buf := cachem.Malloc(int(item.length))
		l.writer.ReadAt(buf, int64(item.offset))
		rec.Unmarshal(buf)
		cachem.Free(buf)
		for _, idx := range idxes {
			if rec.index == idx {
				m[idx] = rec.data
			}
		}
	}
	return m, nil
}

func (l *Log) TruncateFront(idx uint64) error {
	item, err := l.writer.Item(idx)
	if err != nil {
		return err
	}
	l.writer.Remove(HeaderSize, int64(item.offset))
	h, _ := l.writer.Header()
	h.head = idx
	l.writer.WriteAt(h.Marshal(), 0)

	return nil
}
