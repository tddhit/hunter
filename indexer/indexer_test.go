package indexer

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/tddhit/hunter/types"
	"github.com/tddhit/hunter/util"
)

func TestIndexer(t *testing.T) {
	util.InitStopwords("/Users/tangdandang/code/go/src/github.com/huichen/sego/data/stopwords.dict")
	fmt.Println(len(util.Stopwords))
	idx := NewIndexer()
	title := []byte("绿箭侠是一部由剧作家/制片人 Greg Berlanti、Marc Guggenheim和Andrew Kreisberg创作的电视连续剧。它基于DC漫画角色绿箭侠，一个由Mort Weisinger和George Papp创作的装扮奇特的犯罪打击战士。")
	idx.IndexDocument(types.Document{Title: title})
	fmt.Println(string(title))
	for k, l := range idx.Dict {
		if _, ok := util.Stopwords[k]; ok {
			continue
		}
		fmt.Println(k, k == " ")
		var next *list.Element
		for e := l.Front(); e != nil; e = next {
			fmt.Println(*e.Value.(*types.Posting))
			next = e.Next()
		}
	}
}
