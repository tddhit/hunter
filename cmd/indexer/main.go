package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/tddhit/hunter/bindex"
	"github.com/tddhit/hunter/indexer"
	"github.com/tddhit/hunter/types"
	"github.com/tddhit/hunter/util"
)

const (
	TimeFormat = "2006/01/02"
)

func flushIndex(idx *indexer.Indexer, bindex *bindex.BIndex, invertedFile string) {
	file, err := os.OpenFile(invertedFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	for k, v := range idx.Dict {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint64(v.Len()))
		for e := v.Front(); e != nil; e = e.Next() {
			if posting, ok := e.Value.(*types.Posting); ok {
				binary.Write(buf, binary.LittleEndian, posting.DocId)
				binary.Write(buf, binary.LittleEndian, posting.Freq)
				for _, loc := range posting.Loc {
					binary.Write(buf, binary.LittlenEndian, loc)
				}
			} else {
				panic(err)
			}
		}
		n, err := file.Write(buf.Bytes())
		if n != buf.Len() || err != nil {
			util.LogFatalf("flushIndex fail:n=%d,len=%d,err=%s\n", n, buf.Len(), err.String())
		}
		bindex.Put([]byte(k), []byte(strconv.FormatInt(n, 10)))
	}
	file.Close()
}

func indexDocument(idx *indexer.Indexer, documentPath string) {
	var docId uint64 = 0
	file, err := os.Open(documentPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, cap(buf))
	for scanner.Scan() {
		data := strings.Split(scanner.Text(), "\t")
		if len(data) != 9 {
			continue
		}
		doc := &types.Document{
			Id: docId,
		}
		ivalue := reflect.ValueOf(doc)
		for _, kv := range data {
			pair := strings.SplitN(kv, "=", 2)
			if len(pair) != 2 {
				continue
			}
			k, v := pair[0], pair[1]
			if f := ivalue.Elem().FieldByName(k); f.Kind() == reflect.String {
				f.SetString(v)
			} else if f.Kind() == reflect.Uint64 {
				if k == "CreateDate" {
					ts, _ := time.Parse(TimeFormat, v)
					f.SetUint(uint64(ts.Unix()))
				} else {
					t, _ := strconv.ParseUint(v, 10, 64)
					f.SetUint(t)
				}
			}
		}
		idx.IndexDocument(doc)
		docId++
	}
}

func main() {
	dictPath := os.Args[1]
	stopwordsPath := os.Args[2]
	documentPath := os.Args[3]
	vocabFile := os.Args[4]
	invertedFile := os.Args[5]
	idx := indexer.New(dictPath)
	bnx, err := bindex.New(vocabFile, false)
	if err != nil {
		panic(err)
	}
	util.InitStopwords(stopwordsPath)
	indexDocument(idx, documentPath)
	flushIndex(idx, bnx, invertedFile)
}
