//go:build !windows

// Copyright (c) 2022 mobus sunsc0220@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/sunvim/utils/cachem"
)

type UnixFile struct {
	mu      sync.RWMutex
	opts    *Option
	name    string
	offset  int64
	size    int64
	mmpSize uint64
	file    *os.File
	ref     []byte
}

const (
	defaultMemMapSize = 1 << 30
)

func OpenFile(path string, opts *Option) (IFile, error) {
	if opts == nil {
		opts = defaultOption
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}
	uf := &UnixFile{file: f, opts: opts, name: filepath.Base(path)}
	uf.mmap()
	uf.Write(defaultHeader.Marshal())

	return uf, nil
}

func (f *UnixFile) Remove(stx, end int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	buf := make([]byte, f.size-end, f.size-end)
	copy(buf, f.ref[end:])
	copy(f.ref[stx:], buf)
	diff := end - stx
	f.size = f.size - diff
	f.offset = f.offset - diff
	buf = nil // for gc
}

func (f *UnixFile) Check() error {
	h, err := f.Header()
	if err != nil {
		return err
	}
	if h.magic != defaultHeader.magic || h.version != defaultHeader.version {
		return ErrFile
	}
	return nil
}

func (f *UnixFile) Header() (*header, error) {
	hs := cachem.Malloc(HeaderSize)
	defer cachem.Free(hs)
	_, err := f.ReadAt(hs, 0)
	if err != nil {
		return nil, ErrFile
	}
	h := &header{}
	err = h.Unmarshal(hs)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (f *UnixFile) First() (*Record, error) {
	f.Seek(HeaderSize, io.SeekStart)
	r := &Record{}
	rs := cachem.Malloc(RecordSize)
	defer cachem.Free(rs)
	_, err := f.Read(rs)
	if err != nil {
		return nil, ErrInvalidData
	}
	rsize := binary.BigEndian.Uint32(rs)
	records := cachem.Malloc(int(rsize))
	defer cachem.Free(records)
	_, err = f.Read(records)
	if err != nil {
		return nil, ErrInvalidData
	}
	err = r.Unmarshal(records)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (f *UnixFile) Last() (*Record, error) {
	f.Seek(RecordSize, io.SeekEnd)
	r := &Record{}
	rs := cachem.Malloc(RecordSize)
	defer cachem.Free(rs)
	_, err := f.Read(rs)
	if err != nil {
		return nil, ErrInvalidData
	}
	rsize := binary.BigEndian.Uint32(rs) + RecordSize
	records := cachem.Malloc(int(rsize))
	defer cachem.Free(records)
	f.Seek(int64(rsize), io.SeekEnd)
	_, err = f.Read(records)
	if err != nil {
		return nil, ErrInvalidData
	}
	err = r.Unmarshal(records[RecordSize:])
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (f *UnixFile) Close() error {
	f.munmap()
	return f.file.Close()
}

func (f *UnixFile) Read(p []byte) (n int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	copy(p, f.ref[f.offset:])
	n = len(p)
	f.offset += int64(n)
	return
}

func (f *UnixFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	copy(p, f.ref[off:])
	return len(p), nil
}

func (f *UnixFile) Seek(offset int64, whence int) (int64, error) {
	switch {
	case whence == io.SeekStart:
		f.offset = offset
	case whence == io.SeekEnd:
		f.offset = f.size - offset
	case whence == io.SeekCurrent:
		f.offset += offset
	}
	return f.offset, nil
}

func (f *UnixFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	wn := len(p)
	if f.offset+int64(wn) > int64(f.mmpSize) {
		return 0, ErrOutOfSize
	}
	if f.offset+int64(wn) > f.size {
		f.size = f.offset + int64(wn)
		f.grow(f.size)
	}
	copy(f.ref[f.offset:], p)
	f.offset += int64(wn)
	return wn, nil
}

type FileInfo struct {
	Offset uint64
	Size   uint64
	Header []byte
	Name   string
}

func (f *UnixFile) Info() *FileInfo {
	fi := &FileInfo{}
	h, _ := f.Header()
	fi.Header = h.Marshal()
	fi.Name = f.name
	fi.Offset = uint64(f.offset)
	fi.Size = uint64(f.size)
	return fi
}

func (f *UnixFile) WriteSize(size uint32) {
	bs := cachem.Malloc(4)
	defer cachem.Free(bs)
	binary.BigEndian.PutUint32(bs, size)
	f.Write(bs)
}

func (f *UnixFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	wn := len(p)
	if off+int64(wn) > int64(f.mmpSize) {
		return 0, ErrOutOfSize
	}

	if off+int64(wn) > f.size {
		f.size = off + int64(wn)
		f.grow(f.size)
	}
	copy(f.ref[off:], p)

	return wn, nil
}

func (f *UnixFile) Item(idx uint64) (*Item, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	rsizes := cachem.Malloc(RecordSize)
	defer cachem.Free(rsizes)
	indexs := cachem.Malloc(IndexSize)
	defer cachem.Free(indexs)
	var err error
	item := &Item{}
	var pos int64 = HeaderSize

	for {
		f.ReadAt(rsizes, pos)
		rsize := binary.BigEndian.Uint32(rsizes)

		if rsize == 0 {
			err = ErrNotFound
			break
		}

		f.ReadAt(indexs, pos+RecordSize)
		index := binary.BigEndian.Uint64(indexs)
		if index == idx {
			item.offset = uint64(pos)
			item.length = uint64(rsize) + RecordSize
			item.index = index
			break
		}
		pos += int64(rsize) + RecordSize
	}
	return item, err
}

func (f *UnixFile) Items() ([]*Item, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	rsizes := cachem.Malloc(RecordSize)
	defer cachem.Free(rsizes)
	indexs := cachem.Malloc(IndexSize)
	defer cachem.Free(indexs)

	var pos int64 = HeaderSize
	res := make([]*Item, 0)
	for {
		if pos >= f.size {
			break
		}
		f.ReadAt(rsizes, pos)
		rsize := binary.BigEndian.Uint32(rsizes)

		if rsize == 0 {
			break
		}

		f.ReadAt(indexs, pos+RecordSize)
		index := binary.BigEndian.Uint64(indexs)

		res = append(res, &Item{offset: uint64(pos), length: uint64(rsize) + RecordSize, index: index})
		pos += int64(rsize) + RecordSize
	}
	return res, nil
}

// Stat returns os.FileInfo describing the file.
func (f *UnixFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

// Sync commits the current contents of the file.
func (f *UnixFile) Sync() error {
	return f.file.Sync()
}

// Truncate changes the size of the file.
func (f *UnixFile) Truncate(size int64) error {
	err := f.file.Truncate(size)
	if err != nil {
		return err
	}
	f.size = size
	return nil
}

func (f *UnixFile) mmap() {
	var (
		b   []byte
		err error
	)
	if f.opts.MmapSize > defaultMemMapSize {
		f.mmpSize = f.opts.MmapSize
	} else {
		f.mmpSize = defaultMemMapSize
	}
	b, err = syscall.Mmap(int(f.file.Fd()), 0, int(f.mmpSize), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic("mmap failed: " + err.Error())
	}
	f.ref = b
}

func (f *UnixFile) grow(size int64) {
	if info, _ := f.file.Stat(); info.Size() >= size {
		return
	}
	f.file.Truncate(size)
}

func (f *UnixFile) munmap() {
	syscall.Munmap(f.ref)
	f.ref = nil
}
