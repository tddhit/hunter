package builder

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"

	"github.com/tddhit/bindex"
	"github.com/tddhit/tools/log"

	"github.com/tddhit/hunter/indexer"
	"github.com/tddhit/hunter/internal/datasource"
	"github.com/tddhit/hunter/preprocessor"
	"github.com/tddhit/hunter/types"
)

const (
	BM25_K1    float32 = 1.2
	BM25_B     float32 = 0.75
	TimeFormat         = "2006/01/02"
)

type Option struct {
	SegmentPath  string
	StopwordPath string
	VocabPath    string
	InvertPath   string
}

type Builder struct {
	opt        *Option
	proc       *preprocessor.Preprocessor
	indexer    *indexer.Indexer
	dataSource datasource.DataSource

	numDocs      uint64
	avgDocLength uint32
	docLength    map[uint64]uint32
}

func New(option *Option) *Builder {
	b := &Builder{
		opt:     option,
		indexer: indexer.New(),
	}
	proc, err := preprocessor.New(option.SegmentPath, option.StopwordPath)
	if err != nil {
		log.Fatal(err)
	}
	b.proc = proc
	return b
}

func (b *Builder) Reset(source datasource.DataSource) {
	b.numDocs = 0
	b.avgDocLength = 0
	b.docLength = make(map[uint64]uint32)
	b.indexer.Reset()
	b.dataSource = source
}

func (b *Builder) Build() {
	var (
		totalDocLength uint64 = 0
	)
	for doc := range b.dataSource.ReadChan() {
		doc.Terms = b.proc.Segment([]byte(doc.Content))
		docLength := len(doc.Terms)
		totalDocLength += uint64(docLength)
		b.docLength[doc.ID] = uint32(docLength)
		b.indexer.Index(doc)
		b.numDocs++
	}
	b.avgDocLength = uint32(totalDocLength / b.numDocs)
	log.Infof("NumDocs:%d\tAvgDocLength:%d\t", b.numDocs, b.avgDocLength)
}

func (b *Builder) Dump() {
	// init file
	vocabPath := fmt.Sprintf("%s.%d.tmp", b.opt.VocabPath, rand.Int())
	vocab, err := bindex.New(vocabPath, false)
	if err != nil {
		log.Fatal(err)
	}
	invertPath := fmt.Sprintf("%s.%d.tmp", b.opt.InvertPath, rand.Int())
	invert, err := os.OpenFile(invertPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// dump vocab/invert
	var loc uint64
	for term, postingList := range b.indexer.InvertList {
		var buf bytes.Buffer
		err := binary.Write(&buf, binary.LittleEndian, uint64(postingList.Len()))
		if err != nil {
			log.Fatal(err)
		}
		for e := postingList.Front(); e != nil; e = e.Next() {
			if posting, ok := e.Value.(*types.Posting); ok {
				err := binary.Write(&buf, binary.LittleEndian, posting.DocID)
				if err != nil {
					log.Fatal(err)
				}
				idf := float32(math.Log2(float64(b.numDocs)/float64(postingList.Len()) + 1))
				bm25 := idf * float32(posting.Freq) * (BM25_K1 + 1) / (float32(posting.Freq) + BM25_K1*(1-BM25_B+BM25_B*float32(b.docLength[posting.DocID])/float32(b.avgDocLength)))
				err = binary.Write(&buf, binary.LittleEndian, bm25)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				log.Fatalf("convert fail:%#v\n", e)
			}
		}
		n, err := invert.Write(buf.Bytes())
		if n != buf.Len() || err != nil {
			log.Fatalf("dump fail:n=%d,len=%d,err=%s\n", n, buf.Len(), err)
		}
		vocab.Put([]byte(term), []byte(strconv.FormatUint(loc, 10)))
		loc += uint64(n)
	}
	vocab.Close()
	invert.Close()
	os.Rename(vocabPath, b.opt.VocabPath)
	os.Rename(invertPath, b.opt.InvertPath)
}
