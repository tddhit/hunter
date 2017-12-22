package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Conf struct {
	LogLevel     int    `yaml:"loglevel"`
	LogPath      string `yaml:"logpath"`
	SegmentPath  string `yaml:"segment"`
	StopwordPath string `yaml:"stopword"`
	MetaPath     string `yaml:"meta"`
	VocabPath    string `yaml:"vocab"`
	InvertPath   string `yaml:"invert"`
}

func NewConf(path string) (*Conf, error) {
	c := &Conf{}
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(file, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
