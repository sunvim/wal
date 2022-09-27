package wal

import (
	"fmt"
	"os"
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
		err = l.Write(v.index, v.data)
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
		err = l.Write(v.index, v.data)
		assert.Equal(t, nil, err, "succeed")
	}

	d, err := l.Read(tables[1].index)
	assert.Equal(t, nil, err, fmt.Sprintf("index: %d ", tables[1].index))
	if err == nil {
		assert.Equal(t, tables[1].data, d, "should got")
	}
}
