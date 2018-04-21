package indexer

import (
	"bytes"
	"container/heap"
	"container/list"
	"encoding/binary"
	"math"
	"os"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/tddhit/bindex"
	"github.com/tddhit/tools/log"

	"github.com/tddhit/hunter/types"
)

const (
	BM25_K1    float32 = 1.2
	BM25_B     float32 = 0.75
	MaxMapSize         = 1 << 37
)

type Indexer struct {
	NumDocs      uint64
	AvgDocLength uint32
	InvertList   map[string]*list.List
	invertRef    []byte
	invertFile   *os.File
	meta         *bindex.BIndex
	vocab        *bindex.BIndex
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
		if lastestPosting != nil && lastestPosting.DocId == doc.Id {
			posting = lastestPosting
		} else {
			posting = &types.Posting{}
			postingList.PushBack(posting)
		}
		posting.DocId = doc.Id
		posting.Freq++
		posting.Loc = append(posting.Loc, loc)
		loc++
	}
}

func (idx *Indexer) LoadIndex(metaPath, vocabPath, invertPath string) {
	invertFile, err := os.Open(invertPath)
	if err != nil {
		log.Panic(err)
	}
	buf, err := syscall.Mmap(int(invertFile.Fd()), 0, MaxMapSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Panic(err)
	}
	if _, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), uintptr(syscall.MADV_RANDOM)); err != 0 {
		log.Panic(err)
	}
	meta, err := bindex.New(metaPath, true)
	if err != nil {
		log.Panic(err)
	}
	vocab, err := bindex.New(vocabPath, true)
	if err != nil {
		log.Panic(err)
	}
	idx.invertFile = invertFile
	idx.invertRef = buf
	idx.meta = meta
	idx.vocab = vocab
	numDocs := idx.meta.Get([]byte("NumDocs"))
	if numDocs == nil {
		log.Fatal("NumDocs is nil.")
	}
	nd, err := strconv.ParseUint(string(numDocs), 10, 64)
	if err != nil {
		log.Panic(err)
	}
	idx.NumDocs = nd
	avgDocLength := idx.meta.Get([]byte("AvgDocLength"))
	if avgDocLength == nil {
		log.Panic(err)
	}
	avdl, err := strconv.ParseUint(string(avgDocLength), 10, 32)
	if err != nil {
		log.Panic(err)
	}
	idx.AvgDocLength = uint32(avdl)
}

func (idx *Indexer) Search(keys []string, topk int) (res []*types.Document) {
	log.Error("search")
	term2Docs := make(map[string]int)
	doc2Terms := make(map[uint64]map[string]uint32)
	docHeap := &DocHeap{}
	heap.Init(docHeap)
	for _, key := range keys {
		docs, freqs := idx.lookup([]byte(key))
		term2Docs[key] = len(docs)
		for i, docId := range docs {
			if _, ok := doc2Terms[docId]; !ok {
				doc2Terms[docId] = make(map[string]uint32)
			}
			doc2Terms[docId][key] = freqs[i]
		}
	}
	for docId, terms := range doc2Terms {
		d := &types.Document{
			Id: docId,
		}
		for term, freq := range terms {
			value := idx.meta.Get([]byte(strconv.FormatUint(docId, 10)))
			if value == nil {
				log.Errorf("Get doc(%d) length fail\n", docId)
				continue
			}
			docLength, err := strconv.ParseUint(string(value), 10, 32)
			if err != nil {
				log.Error(string(value), err)
				continue
			}
			idf := float32(math.Log2(float64(idx.NumDocs)/float64(term2Docs[term]) + 1))
			d.BM25 += idf * float32(freq) * (BM25_K1 + 1) / (float32(freq) + BM25_K1*(1-BM25_B+BM25_B*float32(docLength)/float32(idx.AvgDocLength)))
			d.Terms = append(d.Terms, term)
			log.Debugf("docId:%d NumDocs:%d docs:%d, freq:%d, docLength:%d, avdl:%d", docId, idx.NumDocs, term2Docs[term], freq, docLength, idx.AvgDocLength)
		}
		heap.Push(docHeap, d)
	}
	docNum := docHeap.Len()
	for topk > 0 && docNum > 0 {
		res = append(res, heap.Pop(docHeap).(*types.Document))
		topk--
		docNum--
	}
	return res
}

func (idx *Indexer) lookup(key []byte) (docs []uint64, freqs []uint32) {
	value := idx.vocab.Get(key)
	if value == nil {
		return nil, nil
	}
	log.Debug("lookup:", string(key), string(value))
	loc, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		log.Error(err.Error())
		return nil, nil
	}
	var count uint64
	buf := bytes.NewBuffer(idx.invertRef[loc : loc+8 : loc+8])
	err = binary.Read(buf, binary.LittleEndian, &count)
	if err != nil {
		log.Error(err)
	}
	log.Debug("count:", count)
	loc += 8
	for i := 0; i < int(count); i++ {
		var (
			docId uint64
			freq  uint32
		)
		buf := bytes.NewBuffer(idx.invertRef[loc : loc+8 : loc+8])
		err := binary.Read(buf, binary.LittleEndian, &docId)
		if err != nil {
			log.Fatal(err)
		}
		loc += 8
		buf = bytes.NewBuffer(idx.invertRef[loc : loc+4 : loc+4])
		err = binary.Read(buf, binary.LittleEndian, &freq)
		if err != nil {
			log.Fatal(err)
		}
		loc += 4
		docs = append(docs, docId)
		freqs = append(freqs, freq)
		log.Debugf("docId:%d freq:%d\n", docId, freq)
	}
	return docs, freqs
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
