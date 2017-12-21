package types

import ()

type Document struct {
	Id      uint64
	Title   string
	Content string
}

type Posting struct {
	DocId uint64
	Freq  uint32
	Loc   []uint32
}
