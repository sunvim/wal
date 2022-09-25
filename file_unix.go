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
	"sync"
	"syscall"
	"unsafe"

	"github.com/sunvim/utils/cachem"
)

type UnixFile struct {
	mu     sync.RWMutex
	opts   *Option
	offset int64
	size   int64
	file   *os.File
	ref    []byte
}

const (
	defaultMemMapSize = 1 << 30
)

func OpenFile(path string, opts *Option) (IWal, error) {
	if opts == nil {
		opts = defaultOption
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}
	uf := &UnixFile{file: f, opts: opts}
	uf.mmap()
	uf.Write(defaultHeader.Marshal())

	return uf, nil
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
	f.Seek(0, io.SeekStart)

	hl := int(unsafe.Sizeof(defaultHeader))
	hs := cachem.Malloc(hl)
	defer cachem.Free(hs)
	_, err := f.Read(hs)
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
	f.Seek(4, io.SeekStart)
	r := &Record{}
	rs := cachem.Malloc(4)
	defer cachem.Free(rs)
	n, err := f.Read(rs)
	if n != 4 || err != nil {
		return nil, ErrInvalidData
	}
	rsize := binary.BigEndian.Uint32(rs)
	records := cachem.Malloc(int(rsize))
	defer cachem.Free(records)
	n, err = f.Read(records)
	if n != int(rsize) || err != nil {
		return nil, ErrInvalidData
	}
	err = r.Unmarshal(records)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (f *UnixFile) Last() (*Record, error) {
	return nil, nil
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
	if f.offset+int64(wn) > int64(len(f.ref)) {
		return 0, ErrOutOfSize
	}
	f.grow(f.size + int64(wn))
	copy(f.ref[f.offset:], p)
	f.size += int64(wn)
	f.offset += int64(wn)

	return wn, nil
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
	if off+int64(wn) > int64(len(f.ref)) {
		return 0, ErrOutOfSize
	}
	copy(f.ref[off:], p)
	f.size += int64(wn)

	return wn, nil
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
		b, err = syscall.Mmap(int(f.file.Fd()), 0, int(f.opts.MmapSize), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	} else {
		b, err = syscall.Mmap(int(f.file.Fd()), 0, defaultMemMapSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	}
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
