package indexer

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"math"
	"os"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/huichen/sego"

	"github.com/tddhit/bindex"
	"github.com/tddhit/hunter/types"
	"github.com/tddhit/hunter/util"
)

const (
	BM25_K1 float32 = 1.2
	BM25_B  float32 = 0.75
)

type Indexer struct {
	seg          sego.Segmenter
	Dict         map[string]*list.List
	dict         *bindex.BIndex
	invertedFile *os.File
	invertedRef  []byte
	numDocs      uint64
	docLength    map[uint64]uint32
	avgDocLength float32
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
	var loc uint32
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
		var lastestPosting *types.Posting
		var posting *types.Posting
		postingList := idx.Dict[term]
		elem := postingList.Back()
		if elem != nil {
			if p, ok := elem.Value.(*types.Posting); ok {
				lastestPosting = p
			}
		}
		if lastestPosting != nil && lastestPosting.DocId == doc.Id {
			posting = lastestPosting
		} else {
			posting = &types.Posting{}
		}
		posting.DocId = doc.Id
		posting.Freq++
		posting.Loc = append(posting.Loc, loc)
		idx.Dict[term].PushBack(posting)
		loc++
	}
}

type Document struct {
	docId uint64
	bm25  float32
	terms []string
}

func (idx *Indexer) Search(keys []string) (res []Document) {
	term2Docs := make(map[string]int)
	doc2Terms := make(map[uint64]map[string]uint32)
	for _, key := range keys {
		docs, freqs := idx.lookup([]byte(key))
		term2Docs[key] = len(docs)
		for i, doc := range docs {
			if _, ok := doc2Terms[doc]; !ok {
				doc2Terms[doc] = make(map[string]uint32)
			}
			doc2Terms[doc][key] = freqs[i]
		}
	}
	for docId, terms := range doc2Terms {
		d := Document{
			docId: docId,
		}
		for term, freq := range terms {
			idf := float32(math.Log2(float64(idx.numDocs)/float64(term2Docs[term]) + 1))
			d.bm25 += idf * float32(freq) * (BM25_K1 + 1) / (float32(freq) + BM25_K1*(1-BM25_B+BM25_B*float32(idx.docLength[docId])/idx.avgDocLength))
			d.terms = append(d.terms, term)
		}
		res = append(res, d)
	}
	return res
}

func (idx *Indexer) lookup(key []byte) (docs []uint64, freqs []uint32) {
	value := idx.dict.Get(key)
	loc, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		util.LogError(err.Error())
		return
	}
	var count uint64
	buf := bytes.NewBuffer(idx.invertedRef[loc : loc+8 : loc+8])
	binary.Read(buf, binary.LittleEndian, &count)
	for i := 0; i < int(count); i++ {
		var docId uint64
		buf := bytes.NewBuffer(idx.invertedRef[loc+8 : loc+16 : loc+16])
		binary.Read(buf, binary.LittleEndian, &docId)
		docs = append(docs, docId)
		var freq uint32
		buf = bytes.NewBuffer(idx.invertedRef[loc+16 : loc+20 : loc+20])
		binary.Read(buf, binary.LittleEndian, &freq)
		freqs = append(freqs, freq)
	}
	return docs, freqs
}

func (idx *Indexer) mmap(size int) error {
	buf, err := syscall.Mmap(int(idx.invertedFile.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	if _, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), uintptr(syscall.MADV_RANDOM)); err != 0 {
		return err
	}
	idx.invertedRef = buf
	return nil
}
