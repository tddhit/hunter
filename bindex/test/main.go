package main

import (
	"strconv"

	"github.com/tddhit/hunter/bindex"
	"github.com/tddhit/hunter/util"
)

func main() {
	b, err := bindex.New("../hunter.idx", false)
	if err != nil {
		panic(err)
	}
	//for i := 10; i <= 20; i++ {
	//	b.Put([]byte("hello"+strconv.Itoa(i)), []byte("world"+strconv.Itoa(i)))
	//}
	//for i := 10; i <= 15; i++ {
	//	b.Delete([]byte("hello" + strconv.Itoa(i)))
	//}
	for i := 16; i <= 20; i++ {
		util.LogDebug(string(b.Get([]byte("hello" + strconv.Itoa(i)))))
	}
}
