package main

import (
	"flag"
	"fmt"
	"testing"
)

var path string

func init() {
	flag.StringVar(&path, "path", "searcher.yml", "config path")
	flag.Parse()
}

func TestConf(t *testing.T) {
	c, _ := NewConf(path)
	fmt.Println(c)
}
