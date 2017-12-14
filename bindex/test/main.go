package main

import (
	"strconv"

	"github.com/tddhit/hunter/bindex"
)

func main() {
	b, err := bindex.New("../hunter.idx", false)
	if err != nil {
		panic(err)
	}
	for i := 16; i <= 20; i++ {
		b.Get([]byte("hello" + strconv.Itoa(i)))
	}
}
