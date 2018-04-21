package types

type Document struct {
	Id      uint64
	BM25    float32
	Terms   []string
	Content string
}

type Posting struct {
	DocId uint64
	Freq  uint32
	Loc   []uint32
}
