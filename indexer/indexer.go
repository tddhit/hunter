package indexer

import (
	"bytes"
	"container/heap"
	"container/list"
	"encoding/binary"
	"errors"
	"os"
	"strconv"
	"syscall"
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
	doc2Tokens := make(docT)
	docHeap := &DocHeap{}
	heap.Init(docHeap)
	log.Info("phase 1")
	for _, key := range keys {
		idx.lookup([]byte(key), doc2Tokens)
	}
	log.Info("phase 2")
	for docID, tokens := range doc2Tokens {
		d := &types.Document{
			ID: docID,
		}
		for _, bm25 := range tokens {
			d.BM25 += bm25
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
	log.Info("End Search")
	return res
}

type docT map[uint64]map[string]float32 // map[docID]map[term]bm25

func (idx *Indexer) lookup(key []byte, doc2Tokens docT) error {
	value := idx.vocab.Get(key)
	if value == nil {
		log.Error(errNotFoundKey, string(key))
		return errNotFoundKey
	}
	log.Debug("lookup:", string(key), string(value))
	loc, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		log.Error(err)
		return err
	}
	var count uint64
	buf := bytes.NewBuffer(idx.invertRef[loc : loc+8 : loc+8])
	err = binary.Read(buf, binary.LittleEndian, &count)
	if err != nil {
		log.Error(err)
	}
	log.Debug("count:", count)
	loc += 8
	log.Info("start scan", count)
	for i := uint64(0); i < count; i++ {
		var (
			docID uint64
			bm25  float32
		)
		buf := bytes.NewBuffer(idx.invertRef[loc : loc+8 : loc+8])
		err := binary.Read(buf, binary.LittleEndian, &docID)
		if err != nil {
			log.Fatal(err)
		}
		loc += 8
		buf = bytes.NewBuffer(idx.invertRef[loc : loc+4 : loc+4])
		err = binary.Read(buf, binary.LittleEndian, &bm25)
		if err != nil {
			log.Fatal(err)
		}
		loc += 4
		if _, ok := doc2Tokens[docID]; !ok {
			doc2Tokens[docID] = make(map[string]float32)
		}
		doc2Tokens[docID][string(key)] = bm25
		log.Debugf("docID:%d bm25:%d\n", docID, bm25)
	}
	log.Info("end scan")
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
