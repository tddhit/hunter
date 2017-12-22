package main

import (
	"flag"

	"github.com/tddhit/tools/log"

	"github.com/tddhit/hunter/searcher"
)

var (
	confPath string
	query    string
)

func init() {
	flag.StringVar(&confPath, "conf", "searcher.yml", "config file")
	flag.StringVar(&query, "query", "gopher", "")
	flag.Parse()
}

func main() {
	conf, err := NewConf(confPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Init(conf.LogPath, conf.LogLevel)
	option := &searcher.Option{
		SegmentPath:  conf.SegmentPath,
		StopwordPath: conf.StopwordPath,
		MetaPath:     conf.MetaPath,
		VocabPath:    conf.VocabPath,
		InvertPath:   conf.InvertPath,
	}
	s := searcher.New(option)
	s.Search([]byte(query))
}
