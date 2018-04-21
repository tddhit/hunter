package builder

import (
	"testing"

	"github.com/tddhit/bindex"
)

func TestBuilder(t *testing.T) {
	b, err := bindex.New("/Users/tangdandang/code/go/src/meizu.com/onemind/nlpservice/apps/builder/data/meta.bin", false)
	if err != nil {
		panic(err)
	}
	b.Traverse()
	/*
		b, err := bindex.New("/Users/tangdandang/code/go/src/meizu.com/onemind/nlpservice/apps/builder/data/vocab.bin", false)
		if err != nil {
			panic(err)
		}
		b.Traverse()
		file, err := os.OpenFile("/Users/tangdandang/code/go/src/meizu.com/onemind/nlpservice/apps/builder/data/inverted.bin", os.O_RDONLY, 0666)
		if err != nil {
			panic(err)
		}
		var id int64
		for {
			var docId uint64
			b := make([]byte, 8)
			n, eof := file.ReadAt(b, id*8)
			if eof == io.EOF {
				break
			}
			buf := bytes.NewReader(b)
			binary.Read(buf, binary.LittleEndian, &docId)
			log.Info(n, docId)
			id++
		}
	*/
}
