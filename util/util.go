package util

import (
	"bufio"
	"io"
	"os"
	"strings"
)

var Stopwords = make(map[string]bool)

func InitStopwords(path string) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		Stopwords[strings.TrimSpace(line)] = true
	}
}
