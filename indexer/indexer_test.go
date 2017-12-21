package indexer

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/tddhit/hunter/types"
	"github.com/tddhit/hunter/util"
)

const (
	dictPath      = "../dict/dictionary.txt"
	stopwordsPath = "../dict/stopwords.txt"
)

func TestIndexer(t *testing.T) {
	title := "绿箭侠是一部由剧作家/制片人 Greg Berlanti、Marc Guggenheim和Andrew Kreisberg创作的电视连续剧。它基于DC漫画角色绿箭侠，一个由Mort Weisinger和George Papp创作的装扮奇特的犯罪打击战士。"
	idx := New(dictPath)
	util.InitStopwords(stopwordsPath)
	idx.IndexDocument(&types.Document{Title: title})
	for k, l := range idx.Dict {
		fmt.Println(k)
		var next *list.Element
		for e := l.Front(); e != nil; e = next {
			fmt.Println(*e.Value.(*types.Posting))
			next = e.Next()
		}
	}
}
