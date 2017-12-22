package preprocessor

import (
	"flag"
	"fmt"
	"testing"
)

var (
	segmentPath  string
	stopwordPath string
)

func init() {
	flag.StringVar(&segmentPath, "segment", "../dict/segment.txt", "")
	flag.StringVar(&stopwordPath, "stopword", "../dict/stopword.txt", "")
}

func TestPreprocessor(t *testing.T) {
	p, _ := New(segmentPath, stopwordPath)
	fmt.Println(p.Segment([]byte("我是一个中国人呀，一个中国人")))
}
