package index

import (
	"my_bitcask/data"
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), //表示控制btree叶子节点数量
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordsPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	defer bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordsPos { //这个库中的btree读不用加锁
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		defer bt.lock.Unlock()
		return false
	}
	defer bt.lock.Unlock()
	return true
}
