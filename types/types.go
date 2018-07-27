package types

type Document struct {
	ID      uint64
	BM25    float32
	Terms   []string
	Content string
}

type Posting struct {
	DocID uint64
	Freq  uint32
	Loc   []uint32
}
