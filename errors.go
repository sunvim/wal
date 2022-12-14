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

import "errors"

var (
	ErrOutOfSize       = errors.New("out of the file size")
	ErrInvalidData     = errors.New("invalid data")
	ErrFile            = errors.New("error file")
	ErrNotFound        = errors.New("not found")
	ErrOutOfRecordSize = errors.New("out of the record max size")
)
