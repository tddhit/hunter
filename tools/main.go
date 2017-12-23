package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"os"
	"syscall"

	"github.com/tddhit/bindex"
	"github.com/tddhit/tools/log"
)

var (
	metaPath   string
	vocabPath  string
	invertPath string
)

func init() {
	flag.StringVar(&metaPath, "meta", "", "")
	flag.StringVar(&vocabPath, "vocab", "", "")
	flag.StringVar(&invertPath, "invert", "", "")
	flag.Parse()
}

func traverseInvert() {
	invertFile, err := os.Open(invertPath)
	if err != nil {
		log.Panic(err)
	}
	stat, err := os.Stat(invertPath)
	if err != nil {
		log.Panic(err)
	}
	invertRef, err := syscall.Mmap(int(invertFile.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Panic(err)
	}
	var loc uint64
	for {
		var count uint64 = 0
		buf := bytes.NewBuffer(invertRef[loc : loc+8 : loc+8])
		err := binary.Read(buf, binary.LittleEndian, &count)
		if err != nil {
			log.Fatal(err)
		}
		log.Debug(loc, count)
		loc += 8
		for i := 0; i < int(count); i++ {
			var (
				docId uint64
				freq  uint32
			)
			buf := bytes.NewBuffer(invertRef[loc : loc+8 : loc+8])
			err := binary.Read(buf, binary.LittleEndian, &docId)
			if err != nil {
				log.Fatal(err)
			}
			loc += 8
			buf = bytes.NewBuffer(invertRef[loc : loc+4 : loc+4])
			err = binary.Read(buf, binary.LittleEndian, &freq)
			if err != nil {
				log.Fatal(err)
			}
			loc += 4
			log.Debug(docId, freq)
		}
	}
}

func main() {
	if metaPath != "" {
		meta, err := bindex.New(metaPath, true)
		if err != nil {
			log.Fatal(err)
		}
		meta.Traverse()
	}
	if vocabPath != "" {
		vocab, err := bindex.New(vocabPath, true)
		if err != nil {
			log.Fatal(err)
		}
		vocab.Traverse()
	}
	if invertPath != "" {
		traverseInvert()
	}
}
