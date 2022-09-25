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

// Record format:
// rsize(4B)+index(8B)+data(NB)+rsize(4B)
type Record struct {
	index uint64
	data  []byte
	rsize uint32
}

func (r *Record) Marshal() []byte {
	ibs := cachem.Malloc(8)
	binary.BigEndian.PutUint64(ibs, r.index)
	r.rsize = uint32(len(r.data)) + 12
	rbs := cachem.Malloc(4)
	binary.BigEndian.PutUint32(rbs, r.rsize)
	defer func() { cachem.Free(ibs); cachem.Free(rbs) }()
	return append(rbs, append(append(ibs, r.data...), rbs...)...)
}

func (r *Record) Unmarshal(data []byte) error {
	if len(data) < 12 {
		return ErrInvalidData
	}
	r.index = binary.BigEndian.Uint64(data[:8])
	r.rsize = binary.BigEndian.Uint32(data[len(data)-4:])
	r.data = make([]byte, r.rsize-12)
	copy(r.data, data[8:])
	return nil
}
