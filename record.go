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

	"github.com/sunvim/utils/cachem"
)

const (
	RecordSize    = 4
	IndexSize     = 8
	RecordMaxSize = 1 << 31
)

const (
	RecordIns OpType = iota
	RecordDel
)

type OpType int

// Record format:
// rsize(4B)+index(8B)+data(NB)+rsize(4B)
type Record struct {
	index uint64
	data  []byte
	rsize uint32
}

func (r *Record) Marshal() ([]byte, error) {
	ibs := cachem.Malloc(IndexSize)
	binary.BigEndian.PutUint64(ibs, r.index)
	r.rsize = uint32(len(r.data)) + IndexSize + RecordSize
	if r.rsize >= RecordMaxSize {
		return nil, ErrOutOfRecordSize
	}
	rbs := cachem.Malloc(RecordSize)
	binary.BigEndian.PutUint32(rbs, r.rsize)
	defer func() { cachem.Free(ibs); cachem.Free(rbs) }()
	return append(rbs, append(append(ibs, r.data...), rbs...)...), nil
}

func (r *Record) Unmarshal(data []byte) error {
	if len(data) < IndexSize+RecordSize {
		return ErrInvalidData
	}
	r.index = binary.BigEndian.Uint64(data[:IndexSize])
	r.rsize = binary.BigEndian.Uint32(data[len(data)-RecordSize:])
	r.data = make([]byte, r.rsize-IndexSize-RecordSize)
	copy(r.data, data[IndexSize:])
	return nil
}
