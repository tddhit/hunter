package builder

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/tddhit/hunter/bindex"
	"github.com/tddhit/hunter/util"
)

func TestBuilder(t *testing.T) {
	b, err := bindex.New("vocab.dat", false)
	if err != nil {
		panic(err)
	}
	b.Traverse()
	file, err := os.OpenFile("inverted.dat", os.O_RDONLY, 0666)
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
		util.LogInfo(n, docId)
		id++
	}
}
