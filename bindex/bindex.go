package bindex

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/tddhit/hunter/util"
)

var (
	ErrKeyRequired     = errors.New("key required")
	ErrKeyTooLarge     = errors.New("key too large")
	ErrValueTooLarge   = errors.New("value too large")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrChecksum        = errors.New("checksum error")
	ErrInvalid         = errors.New("invalid bindex")
)

const (
	VERSION      = 1
	MAGIC        = 0xFE1DEBFE
	MaxMapSize   = 1 << 32
	MaxKeySize   = 256
	MaxValueSize = 512
)

type BIndex struct {
	root        *node
	nodes       map[pgid]*node
	dataref     []byte
	datasz      int
	data        *[MaxMapSize]byte
	path        string
	file        *os.File
	meta        *meta
	fillFactor  float64
	pageSize    int
	pagePool    sync.Pool
	freelist    *list.List
	currentPgid pgid
}

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	root     pgid
	pgid     pgid
	checksum uint64
}

func New(path string, readOnly bool) (*BIndex, error) {
	b := &BIndex{
		nodes:      make(map[pgid]*node),
		path:       path,
		fillFactor: 0.5,
		freelist:   list.New(),
	}
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	if file, err := os.OpenFile(path, flag|os.O_CREATE, 0666); err != nil {
		return nil, err
	} else {
		b.file = file
	}
	if err := b.flock(!readOnly); err != nil {
		return nil, err
	}
	//b.pageSize = os.Getpagesize()
	b.pageSize = 128
	b.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, b.pageSize)
		},
	}
	if info, err := b.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		if err := b.init(); err != nil {
			return nil, err
		}
	}
	if err := b.mmap(1 << 32); err != nil {
		return nil, err
	}
	b.meta = b.page(0).meta()
	util.LogDebug(b.meta)
	b.currentPgid = b.meta.pgid
	return b, nil
}

func (b *BIndex) munmap() error {
	if b.dataref == nil {
		return nil
	}
	err := syscall.Munmap(b.dataref)
	b.dataref = nil
	b.data = nil
	b.datasz = 0
	return err
}

func (b *BIndex) mmap(minsz int) error {
	info, err := b.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	}
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	buf, err := syscall.Mmap(int(b.file.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	if _, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), uintptr(syscall.MADV_RANDOM)); err != 0 {
		return err
	}
	b.dataref = buf
	b.datasz = size
	b.data = (*[MaxMapSize]byte)(unsafe.Pointer(&buf[0]))
	return nil
}

func (b *BIndex) alloc() pgid {
	currentPgid := b.currentPgid
	b.currentPgid++
	return currentPgid
}

func (b *BIndex) init() error {
	buf := make([]byte, b.pageSize*2)
	p := b.pageInBuffer(buf[:], pgid(0))
	p.id = pgid(0)
	p.flags = MetaPageFlag
	m := p.meta()
	m.magic = MAGIC
	m.version = VERSION
	m.pageSize = uint32(b.pageSize)
	m.root = 1
	m.pgid = 2
	m.checksum = m.sum64()

	p = b.pageInBuffer(buf[:], pgid(1))
	p.id = pgid(1)
	p.flags = LeafPageFlag
	p.count = 0

	if _, err := b.file.WriteAt(buf, 0); err != nil {
		return err
	}
	if err := b.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (b *BIndex) pageInBuffer(buf []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&buf[id*pgid(b.pageSize)]))
}

func (b *BIndex) page(id pgid) *page {
	pos := id * pgid(b.pageSize)
	return (*page)(unsafe.Pointer(&b.data[pos]))
}

func (b *BIndex) pageNode(id pgid) (*page, *node) {
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}
	return b.page(id), nil
}

func (b *BIndex) dump() {
	l := list.New()
	if b.root == nil {
		return
	}
	l.PushBack(b.root)
	for {
		if l.Len() > 0 {
			e := l.Front()
			n := e.Value.(*node)
			n.dump()
			l.Remove(e)
			for i := 0; i < len(n.children); i++ {
				l.PushBack(n.children[i])
			}
		} else {
			break
		}
	}
}

func (b *BIndex) Put(key []byte, value []byte) error {
	util.LogDebug("Put:", string(key))
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}
	c := b.newCursor()
	c.seek(key)
	util.LogDebug(c)
	for i := 0; i < len(c.stack); i++ {
		if c.stack[i].node != nil {
			util.LogDebug(c.stack[i].node.pgid, c.stack[i].index)
		}
	}
	var clone = make([]byte, len(key))
	copy(clone, key)
	util.LogDebug("-----------------------before put start--------------------")
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].inodes)
	}
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].children)
	}
	b.dump()
	c.node().put(clone, clone, value, 0)
	util.LogDebug("-----------------------before put end--------------------")
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].inodes)
	}
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].children)
	}
	b.dump()
	c.node().rebalanceAfterInsert()
	return nil
}

func (b *BIndex) Get(key []byte) []byte {
	c := b.newCursor()
	k, v := c.seek(key)
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

func (b *BIndex) Delete(key []byte) error {
	util.LogDebug("Delete:", string(key))
	c := b.newCursor()
	c.seek(key)
	util.LogDebug(c)
	for i := 0; i < len(c.stack); i++ {
		util.LogDebug(c.stack[i].node.pgid, c.stack[i].index)
	}
	util.LogDebug("-----------------------before delete start--------------------")
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].inodes)
	}
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].children)
	}
	b.dump()
	c.node().del(key)
	util.LogDebug("-----------------------before delete end--------------------")
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].inodes)
	}
	for pgid, _ := range b.nodes {
		util.LogDebug(pgid, b.nodes[pgid].children)
	}
	b.dump()
	c.node().rebalanceAfterDelete()
	return nil
}

func (m *meta) write(bindex *BIndex) error {
	buf := make([]byte, m.pageSize)
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.id = pgid(0)
	p.flags = MetaPageFlag
	mm := p.meta()
	*mm = *m
	mm.root = bindex.root.pgid
	mm.pgid = bindex.currentPgid
	mm.checksum = mm.sum64()
	offset := int64(p.id) * int64(bindex.pageSize)
	if _, err := bindex.file.WriteAt(buf, offset); err != nil {
		return err
	}
	if err := bindex.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

func (m *meta) validate() error {
	if m.magic != MAGIC {
		return ErrInvalid
	} else if m.version != VERSION {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

func (b *BIndex) flock(exclusive bool) error {
	flag := syscall.LOCK_SH
	if exclusive {
		flag = syscall.LOCK_EX
	}
	err := syscall.Flock(int(b.file.Fd()), flag|syscall.LOCK_NB)
	if err == nil {
		return nil
	} else if err != syscall.EWOULDBLOCK {
		return err
	}
	return nil
}

func (b *BIndex) funlock() error {
	return syscall.Flock(int(b.file.Fd()), syscall.LOCK_UN)
}

func (b *BIndex) newCursor() *Cursor {
	c := &Cursor{
		bindex: b,
		stack:  make([]elemRef, 0),
	}
	return c
}

func (b *BIndex) node(pgid pgid, parent *node) *node {
	if n := b.nodes[pgid]; n != nil {
		return n
	}
	n := &node{
		bindex: b,
		parent: parent,
	}
	if parent == nil {
		b.root = n
	} else {
		parent.children = append(parent.children, n)
	}
	var p = b.page(pgid)
	n.read(p)
	b.nodes[pgid] = n
	return n
}
