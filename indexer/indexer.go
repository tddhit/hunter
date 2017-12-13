package indexer

import (
	"container/list"

	"github.com/huichen/sego"

	"github.com/tddhit/hunter/types"
	"github.com/tddhit/hunter/util"
)

type Indexer struct {
	seg  sego.Segmenter
	Dict map[string]*list.List
}

func New(segPath string) *Indexer {
	idx := &Indexer{
		Dict: make(map[string]*list.List),
	}
	idx.seg.LoadDictionary(segPath)
	return idx
}

func (idx *Indexer) IndexDocument(doc *types.Document) {
	segs := idx.seg.Segment([]byte(doc.Title))
	for _, seg := range segs {
		term := seg.Token().Text()
		if term == " " {
			continue
		}
		if _, ok := util.Stopwords[term]; ok {
			continue
		}
		if _, ok := idx.Dict[term]; !ok {
			idx.Dict[term] = list.New()
		}
		idx.Dict[term].PushBack(&types.Posting{DocId: doc.Id})
	}
}
