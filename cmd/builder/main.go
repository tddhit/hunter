package main

import "flag"
import (
	"github.com/tddhit/tools/log"

	"github.com/tddhit/hunter/builder"
)

var confPath string

func init() {
	flag.StringVar(&confPath, "conf", "builder.yml", "config file")
	flag.Parse()
}

func main() {
	conf, err := NewConf(confPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Init(conf.LogPath, conf.LogLevel)
	option := &builder.Option{
		SegmentPath:  conf.SegmentPath,
		StopwordPath: conf.StopwordPath,
		DocumentPath: conf.DocumentPath,
		MetaPath:     conf.MetaPath,
		VocabPath:    conf.VocabPath,
		InvertPath:   conf.InvertPath,
	}
	b := builder.New(option)
	b.Build()
	b.Dump()
}
