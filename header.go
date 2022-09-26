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

const HeaderSize = 32

type header struct {
	version uint64
	magic   uint64
	head    uint64
	tail    uint64
}

var defaultHeader = header{version: 1, magic: 0xfaceface}

func (h *header) Marshal() []byte {
	headSlice := make([]byte, HeaderSize, HeaderSize)
	vs := cachem.Malloc(8)
	defer cachem.Free(vs)
	binary.BigEndian.PutUint64(vs, h.version)
	copy(headSlice[:], vs)
	binary.BigEndian.PutUint64(vs, h.magic)
	copy(headSlice[8:], vs)
	binary.BigEndian.PutUint64(vs, h.head)
	copy(headSlice[16:], vs)
	binary.BigEndian.PutUint64(vs, h.tail)
	copy(headSlice[24:], vs)
	return headSlice
}

func (h *header) Unmarshal(data []byte) error {
	if len(data) < HeaderSize {
		return ErrInvalidData
	}
	h.version = binary.BigEndian.Uint64(data[:8])
	h.magic = binary.BigEndian.Uint64(data[8:16])
	h.head = binary.BigEndian.Uint64(data[16:24])
	h.tail = binary.BigEndian.Uint64(data[24:])
	return nil
}
