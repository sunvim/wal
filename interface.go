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
	"io"
	"os"
)

type IWal interface {
	Close() error
	Write(idx uint64, data []byte)
	Read(idx uint64) (data []byte, err error)
	TruncateFront(idx uint64) error
}

type IFile interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Writer
	io.WriterAt

	// Stat returns os.FileInfo describing the file.
	Stat() (os.FileInfo, error)

	// Sync commits the current contents of the file.
	Sync() error

	// Truncate changes the size of the file.
	Truncate(size int64) error

	// Check check file format
	Check() error

	// First Record
	First() (*Record, error)

	// Last Record
	Last() (*Record, error)

	// write size
	WriteSize(size uint32)

	// Header
	Header() (*header, error)

	// Items
	Items() ([]*Item, error)

	// Item
	Item(idx uint64) (*Item, error)

	// Remove
	Remove(stx, end int64)
}
