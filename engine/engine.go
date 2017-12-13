package engine

import (
	"github.com/tddhit/hunter/indexer"
	"github.com/tddhit/hunter/segmenter"
)

type Engine struct {
	segmenter *segmenter.Segmenter
	indexer   *indexer.Indexer
}

func NewEngine() *Engine {
	eng := &Engine{}
	return eng
}

func (e *Engine) IndexDocument(docId uint64, doc types.Document) {
}

func (e *Engine) Search() {
}
