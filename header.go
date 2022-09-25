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

type header struct {
	version uint16
	magic   uint16
}

var defaultHeader = header{version: 1, magic: 0xface}

func (h *header) Marshal() []byte {
	vs := cachem.Malloc(2)
	defer cachem.Free(vs)
	binary.BigEndian.PutUint16(vs, h.version)
	ms := cachem.Malloc(2)
	binary.BigEndian.PutUint16(ms, h.magic)
	return append(vs, ms...)
}

func (h *header) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return ErrInvalidData
	}
	h.version = binary.BigEndian.Uint16(data[:2])
	h.magic = binary.BigEndian.Uint16(data[2:4])
	return nil
}
