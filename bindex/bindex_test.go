package bindex

import (
	"strconv"
	"testing"

	"github.com/tddhit/hunter/util"
)

func TestBIndex(t *testing.T) {
	b, err := New("hunter.idx", false)
	if err != nil {
		panic(err)
	}
	//for i := 1000; i <= 1200; i++ {
	//	b.Put([]byte("hello"+strconv.Itoa(i)), []byte("world"+strconv.Itoa(i)))
	//}
	//for i := 1000; i <= 1190; i++ {
	//	b.Delete([]byte("hello" + strconv.Itoa(i)))
	//}
	for i := 1000; i <= 1230; i++ {
		util.LogDebug(string(b.Get([]byte("hello" + strconv.Itoa(i)))))
	}
	//for i := 16; i <= 20; i++ {
	//	b.Delete([]byte("hello" + strconv.Itoa(i)))
	//}
	//b.Delete([]byte("hello" + strconv.Itoa(20)))
	//util.LogDebug(string(b.Get([]byte("hello" + strconv.Itoa(20)))))
	util.LogDebug("")
}
