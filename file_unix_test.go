package wal

import (
	"bytes"
	"os"
	"testing"

	"github.com/sunvim/utils/cachem"
)

var (
	testfile = "text.file"

	tables = []*Record{
		{
			index: 1,
			data:  []byte("hello"),
		},
		{
			index: 2,
			data:  []byte("mobus"),
		},
		{
			index: 3,
			data:  []byte("world,hello"),
		},
		{
			index: 4,
			data:  []byte("my family"),
		},
	}
)

func openFile(path string) IFile {

	os.RemoveAll(testfile)

	uf, err := OpenFile(testfile, nil)
	if err != nil {
		panic(err)
	}
	return uf
}

func TestOpenFile(t *testing.T) {
	os.RemoveAll(testfile)
	_, err := OpenFile(testfile, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestHeader(t *testing.T) {

	h := &header{
		version: 1,
		magic:   0xfa,
	}
	hs := h.Marshal()
	ha := &header{}
	ha.Unmarshal(hs)

	if ha.magic != h.magic || ha.version != h.version {
		t.Error("header failed")
	}

}

func TestRecord(t *testing.T) {
	r := &Record{
		index: 1,
		data:  []byte("hello"),
	}
	rs := r.Marshal()

	rr := &Record{}
	rr.Unmarshal(rs[4:])
	if rr.index != r.index || rr.rsize != r.rsize || !bytes.Equal(rr.data, r.data) {

		t.Errorf("rsize want: %d got: %d \n index want: %d got: %d \n data want: %s got: %s \n",
			r.rsize, rr.rsize,
			r.index, rr.index,
			r.data, rr.data)
		return
	}
}

func TestFirstRecord(t *testing.T) {
	uf := openFile(testfile)
	r := &Record{
		index: 1,
		data:  []byte("hello"),
	}

	uf.Write(r.Marshal())

	rr, err := uf.First()
	if err != nil {
		t.Error(err)
		return
	}
	if r.rsize != rr.rsize || r.index != rr.index || !bytes.Equal(r.data, rr.data) {
		t.Errorf("rsize want: %d got: %d \n index want: %d got: %d \n data want: %s got: %s \n",
			r.rsize, rr.rsize,
			r.index, rr.index,
			r.data, rr.data)
		return
	}
}

func TestItems(t *testing.T) {

	uf := openFile(testfile)
	for _, v := range tables {
		uf.Write(v.Marshal())
	}
	r := &Record{}
	items, _ := uf.Items()
	for _, item := range items {
		buf := cachem.Malloc(int(item.length))
		uf.ReadAt(buf, int64(item.offset))
		r.Unmarshal(buf[4:])
		cachem.Free(buf)
		for _, v := range tables {
			if v.index == r.index {
				if v.rsize != r.rsize || !bytes.Equal(v.data, r.data) {
					t.Error("not matched")
				}
			}
		}
	}
}

func TestRemove(t *testing.T) {
	uf := openFile(testfile)
	for _, v := range tables {
		uf.Write(v.Marshal())
	}

	items, _ := uf.Items()
	for _, item := range items {
		t.Logf("item: %+v \n", item)
	}
	stx := items[1].offset
	end := items[2].offset + items[2].length
	uf.Remove(int64(stx), int64(end))

	items, _ = uf.Items()
	for _, item := range items {
		t.Logf("item: %+v \n", item)
	}

}

func TestLastRecord(t *testing.T) {

	uf := openFile(testfile)
	r := &Record{
		index: 1,
		data:  []byte("hello"),
	}

	uf.Write(r.Marshal())

	r = &Record{
		index: 2,
		data:  []byte("mobus"),
	}

	uf.Write(r.Marshal())

	r = &Record{
		index: 3,
		data:  []byte("world"),
	}

	uf.Write(r.Marshal())

	rr, err := uf.Last()
	if err != nil {
		t.Error(err)
		return
	}
	if r.rsize != rr.rsize || r.index != rr.index || !bytes.Equal(r.data, rr.data) {
		t.Errorf("rsize want: %d got: %d \n index want: %d got: %d \n data want: %s got: %s \n",
			r.rsize, rr.rsize,
			r.index, rr.index,
			r.data, rr.data)
		return
	}

}

func BenchmarkUFWrite(b *testing.B) {
	uf := openFile(testfile)
	msg := []byte("hello wal\n")
	b.ResetTimer()
	for i := 0; i < 10000; i++ {
		uf.Write(msg)
	}
}
