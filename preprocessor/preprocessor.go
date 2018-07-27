package preprocessor

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/huichen/sego"

	"github.com/tddhit/tools/log"
)

type Preprocessor struct {
	segmenter sego.Segmenter
	stopwords map[string]bool
}

func New(segmentPath, stopwordPath string) (*Preprocessor, error) {
	_, err := os.Stat(segmentPath)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	_, err = os.Stat(stopwordPath)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	p := &Preprocessor{
		stopwords: make(map[string]bool),
	}
	p.segmenter.LoadDictionary(segmentPath)
	file, err := os.Open(stopwordPath)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer file.Close()
	rd := bufio.NewReader(file)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		p.stopwords[strings.TrimSpace(line)] = true
	}
	return p, nil
}

func (p *Preprocessor) Segment(text []byte) (terms []string) {
	segments := p.segmenter.Segment(text)
	segs := sego.SegmentsToSlice(segments, true)
	for _, seg := range segs {
		if _, ok := p.stopwords[seg]; !ok && seg != " " {
			terms = append(terms, seg)
		}
	}
	return
}
