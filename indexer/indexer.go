package indexer

import (
	"bytes"
	"container/heap"
	"container/list"
	"encoding/binary"
	"errors"
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

var (
	errNotFoundKey = errors.New("bindex not found key")
)

type Indexer struct {
	NumDocs      uint64
	AvgDocLength uint32
	InvertList   map[string]*list.List
	meta0        *bindex.BIndex
	meta1File    *os.File
	meta1Ref     []byte
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

func (idx *Indexer) LoadIndex(meta0Path, meta1Path, vocabPath, invertPath string) {
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

	// mmap meta1file
	meta1File, err := os.Open(meta1Path)
	if err != nil {
		log.Panic(err)
	}
	meta1Buf, err := syscall.Mmap(int(meta1File.Fd()), 0, MaxMapSize,
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Panic(err)
	}
	if _, _, err := syscall.Syscall(syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&meta1Buf[0])), uintptr(len(meta1Buf)),
		uintptr(syscall.MADV_RANDOM)); err != 0 {

		log.Panic(err)
	}

	meta0, err := bindex.New(meta0Path, true)
	if err != nil {
		log.Panic(err)
	}
	vocab, err := bindex.New(vocabPath, true)
	if err != nil {
		log.Panic(err)
	}
	idx.invertFile = invertFile
	idx.invertRef = invertBuf
	idx.meta1File = meta1File
	idx.meta1Ref = meta1Buf
	idx.meta0 = meta0
	idx.vocab = vocab
	numDocs := idx.meta0.Get([]byte("NumDocs"))
	if numDocs == nil {
		log.Fatal("NumDocs is nil.")
	}
	nd, err := strconv.ParseUint(string(numDocs), 10, 64)
	if err != nil {
		log.Panic(err)
	}
	idx.NumDocs = nd
	avgDocLength := idx.meta0.Get([]byte("AvgDocLength"))
	if avgDocLength == nil {
		log.Panic(err)
	}
	avdl, err := strconv.ParseUint(string(avgDocLength), 10, 32)
	if err != nil {
		log.Panic(err)
	}
	idx.AvgDocLength = uint32(avdl)
}

func (idx *Indexer) Search(keys []string, topk int32) (res []*types.Document) {
	log.Info("Start Search")
	term2Docs := make(map[string]uint64)
	doc2Terms := make(docT)
	docHeap := &DocHeap{}
	heap.Init(docHeap)
	log.Info("phase 1")
	for _, key := range keys {
		idx.lookup([]byte(key), term2Docs, doc2Terms)
	}
	log.Info("phase 2", len(doc2Terms))
	for docId, terms := range doc2Terms {
		d := &types.Document{
			Id: docId,
		}
		var docLength uint32
		pos := docId * 4 // legnth: 4 bytes
		buf := bytes.NewBuffer(idx.meta1Ref[pos : pos+4 : pos+4])
		err := binary.Read(buf, binary.LittleEndian, &docLength)
		if err != nil {
			log.Error(err)
			continue
		}
		for term, freq := range terms {
			idf := float32(math.Log2(float64(idx.NumDocs)/float64(term2Docs[term]) + 1))
			d.BM25 += idf * float32(freq) * (BM25_K1 + 1) / (float32(freq) + BM25_K1*(1-BM25_B+BM25_B*float32(docLength)/float32(idx.AvgDocLength)))
			d.Terms = append(d.Terms, term)
			log.Debugf("docId:%d NumDocs:%d docs:%d, freq:%d, docLength:%d, avdl:%d", docId, idx.NumDocs, term2Docs[term], freq, docLength, idx.AvgDocLength)
		}
		heap.Push(docHeap, d)
	}
	log.Info("phase 3")
	docNum := docHeap.Len()
	for topk > 0 && docNum > 0 {
		doc := heap.Pop(docHeap).(*types.Document)
		rawID := idx.meta0.Get([]byte("ID_" + strconv.FormatUint(doc.Id, 10)))
		if rawID == nil {
			log.Error("rawID is nil.", doc.Id)
			continue
		}
		rawDocID, err := strconv.ParseUint(string(rawID), 10, 64)
		if err != nil {
			log.Panic(err)
		}
		doc.Id = rawDocID
		res = append(res, doc)
		topk--
		docNum--
	}
	log.Info("End Search")
	return res
}

type docT map[uint64]map[string]uint32 // map[docID]map[term]freq

func (idx *Indexer) lookup(key []byte, term2Docs map[string]uint64,
	doc2Terms docT) error {

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
	term2Docs[string(key)] = count
	for i := uint64(0); i < count; i++ {
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
		if _, ok := doc2Terms[docId]; !ok {
			doc2Terms[docId] = make(map[string]uint32)
		}
		doc2Terms[docId][string(key)] = freq
		//docs = append(docs, docId)
		//freqs = append(freqs, freq)
		log.Debugf("docId:%d freq:%d\n", docId, freq)
	}
	log.Info("end scan")
	return nil
}

func (idx *Indexer) Reset() error {
	idx.NumDocs = 0
	idx.AvgDocLength = 0
	idx.InvertList = make(map[string]*list.List)
	if idx.meta0 != nil {
		idx.meta0.Close()
		idx.meta0 = nil
	}
	if idx.meta1Ref != nil {
		if err := syscall.Munmap(idx.meta1Ref); err != nil {
			return err
		}
		idx.meta1Ref = nil
	}
	if idx.meta1File != nil {
		idx.meta1File.Sync()
		idx.meta1File.Close()
		idx.meta1File = nil
	}
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
