package builder

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	TimeFormat = "2006/01/02"
)

type Option struct {
	SegmentPath  string
	StopwordPath string
	Meta0Path    string
	Meta1Path    string
	VocabPath    string
	InvertPath   string
}

type Builder struct {
	opt        *Option
	proc       *preprocessor.Preprocessor
	indexer    *indexer.Indexer
	dataSource datasource.DataSource

	NumDocs      uint64
	AvgDocLength uint32
	DocsLength   []struct {
		id     uint64
		length uint32
	}
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
	b.NumDocs = 0
	b.AvgDocLength = 0
	b.DocsLength = b.DocsLength[:]
	b.indexer.Reset()
	b.dataSource = source
}

func (b *Builder) Build() {
	var (
		totalDocLength uint64 = 0
	)
	for doc := range b.dataSource.ReadChan() {
		terms := b.proc.Segment([]byte(doc.Content))
		docLength := len(terms)
		totalDocLength += uint64(docLength)
		b.DocsLength = append(b.DocsLength, struct {
			id     uint64
			length uint32
		}{
			id:     doc.Id,
			length: uint32(docLength),
		})
		doc.Id = b.NumDocs
		doc.Terms = terms
		b.indexer.Index(doc)
		b.NumDocs++
	}
	b.AvgDocLength = uint32(totalDocLength / b.NumDocs)
	log.Infof("NumDocs:%d\tAvgDocLength:%d\t", b.NumDocs, b.AvgDocLength)
}

func (b *Builder) Dump() {
	// init file
	meta0Path := fmt.Sprintf("%s.%d.tmp", b.opt.Meta0Path, rand.Int())
	meta0, err := bindex.New(meta0Path, false)
	if err != nil {
		log.Fatal(err)
	}
	meta1Path := fmt.Sprintf("%s.%d.tmp", b.opt.Meta1Path, rand.Int())
	meta1, err := os.OpenFile(meta1Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
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

	// dump meta0
	meta0.Put([]byte("NumDocs"), []byte(strconv.FormatUint(b.NumDocs, 10)))
	meta0.Put([]byte("AvgDocLength"),
		[]byte(strconv.FormatUint(uint64(b.AvgDocLength), 10)))
	for i, doc := range b.DocsLength {
		meta0.Put([]byte("ID_"+strconv.Itoa(i)),
			[]byte(strconv.FormatUint(doc.id, 10)))
	}

	// dump meta1
	for _, doc := range b.DocsLength {
		var buf bytes.Buffer
		err := binary.Write(&buf, binary.LittleEndian, doc.length)
		if err != nil {
			log.Fatal(err)
		}
		n, err := meta1.Write(buf.Bytes())
		if n != buf.Len() || err != nil {
			log.Fatalf("dump fail:n=%d,len=%d,err=%s\n", n, buf.Len(), err)
		}
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
				err := binary.Write(&buf, binary.LittleEndian, posting.DocId)
				if err != nil {
					log.Fatal(err)
				}
				err = binary.Write(&buf, binary.LittleEndian, posting.Freq)
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
	meta0.Close()
	meta1.Close()
	vocab.Close()
	invert.Close()
	os.Rename(meta0Path, b.opt.Meta0Path)
	os.Rename(meta1Path, b.opt.Meta1Path)
	os.Rename(vocabPath, b.opt.VocabPath)
	os.Rename(invertPath, b.opt.InvertPath)
}
