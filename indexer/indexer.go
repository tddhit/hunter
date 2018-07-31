package indexer

import (
	"container/heap"
	"container/list"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/tddhit/bindex"
	"github.com/tddhit/tools/log"

	"github.com/tddhit/hunter/types"
)

const (
	MaxMapSize = 1 << 37
)

var (
	errNotFoundKey = errors.New("bindex not found key")
)

type Indexer struct {
	NumDocs      uint64
	AvgDocLength uint32
	InvertList   map[string]*list.List
	vocab        *bindex.BIndex
	invertFile   *os.File
	invertRef    []byte
}

func New() *Indexer {
	idx := &Indexer{
		InvertList: make(map[string]*list.List),
	}
	return idx
}

func (idx *Indexer) Index(doc *types.Document) {
	var loc uint32
	for _, term := range doc.Terms {
		var (
			lastestPosting *types.Posting
			posting        *types.Posting
		)
		if _, ok := idx.InvertList[term]; !ok {
			idx.InvertList[term] = list.New()
		}
		postingList := idx.InvertList[term]
		elem := postingList.Back()
		if elem != nil {
			if p, ok := elem.Value.(*types.Posting); ok {
				lastestPosting = p
			}
		}
		if lastestPosting != nil && lastestPosting.DocID == doc.ID {
			posting = lastestPosting
		} else {
			posting = &types.Posting{}
			postingList.PushBack(posting)
		}
		posting.DocID = doc.ID
		posting.Freq++
		posting.Loc = append(posting.Loc, loc)
		loc++
	}
}

func (idx *Indexer) LoadIndex(vocabPath, invertPath string) {
	// mmap invertfile
	invertFile, err := os.Open(invertPath)
	if err != nil {
		log.Panic(err)
	}
	invertBuf, err := syscall.Mmap(int(invertFile.Fd()), 0, MaxMapSize,
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Panic(err)
	}
	if _, _, err := syscall.Syscall(syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&invertBuf[0])), uintptr(len(invertBuf)),
		uintptr(syscall.MADV_RANDOM)); err != 0 {

		log.Panic(err)
	}

	vocab, err := bindex.New(vocabPath, true)
	if err != nil {
		log.Panic(err)
	}
	idx.invertFile = invertFile
	idx.invertRef = invertBuf
	idx.vocab = vocab
}

func (idx *Indexer) Search(keys []string, topk int32) (res []*types.Document) {
	log.Info("Start Search")
	ts := time.Now()
	doc2BM25 := make(map[uint64]float32)
	docHeap := &DocHeap{}
	heap.Init(docHeap)
	log.Info("phase 1")
	for _, key := range keys {
		idx.lookup([]byte(key), doc2BM25)
	}
	log.Info("phase 2")
	for docID, bm25 := range doc2BM25 {
		d := &types.Document{
			ID:   docID,
			BM25: bm25,
		}
		heap.Push(docHeap, d)
	}
	docNum := docHeap.Len()
	log.Info("phase 3", docNum)
	for topk > 0 && docNum > 0 {
		doc := heap.Pop(docHeap).(*types.Document)
		res = append(res, doc)
		topk--
		docNum--
	}
	log.Info("End Search", time.Since(ts), docNum)
	return res
}

func (idx *Indexer) lookup(key []byte, doc2BM25 map[uint64]float32) error {
	value := idx.vocab.Get(key)
	if value == nil {
		log.Error(errNotFoundKey, string(key))
		return errNotFoundKey
	}
	loc := binary.LittleEndian.Uint64(value)
	count := binary.LittleEndian.Uint64(idx.invertRef[loc : loc+8 : loc+8])
	log.Debug("count:", count)
	loc += 8
	for i := uint64(0); i < count; i++ {
		docID := binary.LittleEndian.Uint64(idx.invertRef[loc : loc+8 : loc+8])
		loc += 8
		bits := binary.LittleEndian.Uint32(idx.invertRef[loc : loc+4 : loc+4])
		bm25 := math.Float32frombits(bits)
		loc += 4
		doc2BM25[docID] += bm25
	}
	return nil
}

func (idx *Indexer) Reset() error {
	idx.NumDocs = 0
	idx.AvgDocLength = 0
	idx.InvertList = make(map[string]*list.List)
	if idx.vocab != nil {
		idx.vocab.Close()
		idx.vocab = nil
	}
	if idx.invertRef != nil {
		if err := syscall.Munmap(idx.invertRef); err != nil {
			return err
		}
		idx.invertRef = nil
	}
	if idx.invertFile != nil {
		idx.invertFile.Sync()
		idx.invertFile.Close()
		idx.invertFile = nil
	}
	return nil
}

func (idx *Indexer) Close() {
	idx.Reset()
}

type DocHeap []*types.Document

func (h DocHeap) Len() int           { return len(h) }
func (h DocHeap) Less(i, j int) bool { return h[i].BM25 > h[j].BM25 }
func (h DocHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *DocHeap) Push(x interface{}) {
	*h = append(*h, x.(*types.Document))
}

func (h *DocHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
