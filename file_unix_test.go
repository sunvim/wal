package wal

import (
	"bytes"
	"os"
	"testing"
)

var (
	testfile = "text.file"
)

func openFile(path string) IWal {

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

func BenchmarkUFWrite(b *testing.B) {
	uf := openFile(testfile)
	msg := []byte("hello wal\n")
	b.ResetTimer()
	for i := 0; i < 10000; i++ {
		uf.Write(msg)
	}
}
