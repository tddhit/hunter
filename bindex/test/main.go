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
	for i := 1; i <= 10000000; i++ {
		util.LogInfo(string(b.Get([]byte("hello" + strconv.Itoa(i)))))
	}
	//for i := 1; i <= 10000000; i++ {
	//	b.Put([]byte("hello"+strconv.Itoa(i)), []byte("world"+strconv.Itoa(i)))
	//	util.LogInfo(i)
	//}
	//for i := 1; i <= 10000000; i++ {
	//	b.Delete([]byte("hello" + strconv.Itoa(i)))
	//	util.LogInfo(i)
	//}
}
