package builder

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"strconv"

	"github.com/tddhit/bindex"
	"github.com/tddhit/tools/log"

	"github.com/tddhit/hunter/indexer"
	"github.com/tddhit/hunter/preprocessor"
	"github.com/tddhit/hunter/types"
)

const (
	TimeFormat = "2006/01/02"
)

type Option struct {
	SegmentPath  string
	StopwordPath string
	DocumentPath string
	MetaPath     string
	VocabPath    string
	InvertPath   string
}

type Builder struct {
	opt     *Option
	doc     *os.File
	proc    *preprocessor.Preprocessor
	indexer *indexer.Indexer
	vocab   *bindex.BIndex
	meta    *bindex.BIndex
	invert  *os.File

	NumDocs      uint64
	AvgDocLength uint32
	DocLength    map[uint64]uint32
}

func New(option *Option) *Builder {
	b := &Builder{
		opt:       option,
		indexer:   indexer.New(),
		DocLength: make(map[uint64]uint32),
	}
	meta, err := bindex.New(option.MetaPath, false)
	if err != nil {
		log.Panic(err)
	}
	vocab, err := bindex.New(option.VocabPath, false)
	if err != nil {
		log.Panic(err)
	}
	invert, err := os.OpenFile(option.InvertPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Panic(err)
	}
	doc, err := os.Open(option.DocumentPath)
	if err != nil {
		log.Panic(err)
	}
	proc, err := preprocessor.New(option.SegmentPath, option.StopwordPath)
	if err != nil {
		log.Panic(err)
	}
	b.meta = meta
	b.vocab = vocab
	b.invert = invert
	b.doc = doc
	b.proc = proc
	return b
}

func (b *Builder) Build() {
	var (
		docId          uint64 = 0
		totalDocLength uint64 = 0
	)
	scanner := bufio.NewScanner(b.doc)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, cap(buf))
	for scanner.Scan() {
		content := scanner.Text()
		terms := b.proc.Segment([]byte(content))
		docLength := len(terms)
		totalDocLength += uint64(docLength)
		b.DocLength[docId] = uint32(docLength)
		doc := &types.Document{
			Id:    docId,
			Terms: terms,
		}
		b.indexer.Index(doc)
		docId++
	}
	b.NumDocs = docId
	b.AvgDocLength = uint32(totalDocLength / docId)
}

func (b *Builder) Dump() {
	b.meta.Put([]byte("NumDocs"), []byte(strconv.FormatUint(b.NumDocs, 10)))
	b.meta.Put([]byte("AvgDocLength"), []byte(strconv.FormatUint(uint64(b.AvgDocLength), 10)))
	for docId, length := range b.DocLength {
		b.meta.Put([]byte(strconv.FormatUint(docId, 10)), []byte(strconv.FormatUint(uint64(length), 10)))
	}
	var loc uint64
	for term, postingList := range b.indexer.InvertList {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, uint64(postingList.Len()))
		if err != nil {
			log.Fatal(err)
		}
		for e := postingList.Front(); e != nil; e = e.Next() {
			if posting, ok := e.Value.(*types.Posting); ok {
				err := binary.Write(buf, binary.LittleEndian, posting.DocId)
				if err != nil {
					log.Fatal(err)
				}
				err = binary.Write(buf, binary.LittleEndian, posting.Freq)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				log.Panicf("convert fail:%#v\n", e)
			}
		}
		n, err := b.invert.Write(buf.Bytes())
		if n != buf.Len() || err != nil {
			log.Fatalf("dump fail:n=%d,len=%d,err=%s\n", n, buf.Len(), err.Error())
		}
		b.vocab.Put([]byte(term), []byte(strconv.FormatUint(loc, 10)))
		loc += uint64(n)
	}
}
