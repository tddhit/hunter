package searcher

import (
	"github.com/tddhit/tools/log"

	"github.com/tddhit/hunter/indexer"
	"github.com/tddhit/hunter/preprocessor"
	"github.com/tddhit/hunter/types"
)

type Option struct {
	SegmentPath  string
	StopwordPath string
	MetaPath     string
	VocabPath    string
	InvertPath   string
}

type Searcher struct {
	proc    *preprocessor.Preprocessor
	indexer *indexer.Indexer
}

func New(option *Option) *Searcher {
	s := &Searcher{
		indexer: indexer.New(),
	}
	s.indexer.LoadIndex(option.MetaPath, option.VocabPath, option.InvertPath)
	proc, err := preprocessor.New(option.SegmentPath, option.StopwordPath)
	if err != nil {
		log.Panic(err)
	}
	s.proc = proc
	return s
}

func (s *Searcher) Search(query []byte, topk int) []*types.Document {
	terms := s.proc.Segment(query)
	return s.indexer.Search(terms, topk)
}
