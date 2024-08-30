package index

import (
	"bytes"
	"my_bitcask/data"
	"sort"
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

// btree 索引迭代器
type bTreeIterator struct {
	currIndex int     //当前遍历到哪个key
	reverse   bool    //是否要反向遍历
	values    []*Item //key位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *bTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	//将所有数据放在数组中
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &bTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.Unlock()
	return newBTreeIterator(bt.tree, reverse)
}

// Rewind 重新回到迭代器起点
func (bti *bTreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的key返回第一个大于（或小于）等于key的item
func (bti *bTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0 //byte.compare返回 0：如果 a 和 b 相等。 返回 -1：如果 a 小于 b
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}

}

// Next 返回下一个item
func (bti *bTreeIterator) Next() {
	bti.currIndex++
}

// Valid 判断迭代器是否有效,用于推出遍历
func (bti *bTreeIterator) Valid() bool {
	return bti.currIndex <= len(bti.values)
}

// Key 当前位置Key数据
func (bti *bTreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的value数据
func (bti *bTreeIterator) Value() *data.LogRecordsPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器，释放资源
func (bti *bTreeIterator) Close() {
	bti.values = nil
}
