package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/tddhit/hunter/indexer"
	"github.com/tddhit/hunter/types"
	"github.com/tddhit/hunter/util"
)

const (
	TimeFormat = "2006/01/02"
)

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
		fmt.Println(doc.Title)
		idx.IndexDocument(doc)
		docId++
	}
	for k, v := range idx.Dict {
		fmt.Println(k)
		for e := v.Front(); e != nil; e = e.Next() {
			fmt.Println(*e.Value.(*types.Posting))
		}
	}
}

func main() {
	dictPath := os.Args[1]
	stopwordsPath := os.Args[2]
	documentPath := os.Args[3]
	idx := indexer.New(dictPath)
	util.InitStopwords(stopwordsPath)
	indexDocument(idx, documentPath)
}
