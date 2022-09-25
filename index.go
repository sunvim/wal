package wal

type Item struct {
	offset uint64
	index  uint64
	length uint64
}
