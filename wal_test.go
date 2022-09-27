package wal

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/assert"
)

func TestWalOpen(t *testing.T) {

	os.RemoveAll(testfile)

	l, err := Open(testfile, nil)
	if err != nil {
		t.Error(err)
	}

	assert.NotEqual(t, nil, l.writer, "writter is nill")

}

func TestWalWrite(t *testing.T) {
	os.RemoveAll(testfile)
	l, err := Open(testfile, nil)
	if err != nil {
		t.Error(err)
	}

	for _, v := range tables {
		err = l.Write(v.data)
		assert.Equal(t, nil, err, "succeed")
	}

}

func TestWalRead(t *testing.T) {
	os.RemoveAll(testfile)
	l, err := Open(testfile, nil)
	if err != nil {
		t.Error(err)
	}

	for _, v := range tables {
		err = l.Write(v.data)
		assert.Equal(t, nil, err, "succeed")
	}

	d, err := l.Read(tables[1].index)
	assert.Equal(t, nil, err, fmt.Sprintf("index: %d ", tables[1].index))
	if err == nil {
		assert.Equal(t, tables[1].data, d, "should got")
	}
}

func TestWalReadBatch(t *testing.T) {
	os.RemoveAll(testfile)
	l, err := Open(testfile, nil)
	if err != nil {
		t.Error(err)
	}

	for _, v := range tables {
		err = l.Write(v.data)
		assert.Equal(t, nil, err, "succeed")
	}

	items, err := l.ReadBatch(1, 3, 4)

	for k, v := range items {
		assert.Equal(t, tables[k].data, v, fmt.Sprintf("index: %d ", k))
	}

}

func TestWalTruncateFront(t *testing.T) {

	os.RemoveAll(testfile)
	l, err := Open(testfile, nil)
	if err != nil {
		t.Error(err)
	}

	for _, v := range tables {
		l.Write(v.data)
	}
	items, err := l.writer.Items()
	for _, v := range items {
		d, _ := l.Read(v.index)
		t.Logf("index: %d data: %s \n", v.index, d)
	}

	l.TruncateFront(3)
	t.Log(strings.Repeat("==", 20))
	items, err = l.writer.Items()
	assert.Equal(t, nil, err, "read rest item failed")
	for _, v := range items {
		d, _ := l.Read(v.index)
		t.Logf("index: %d data: %s \n", v.index, d)
	}
}
