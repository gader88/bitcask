package index

import (
	"bytes"
	"my_bitcask/data"

	"github.com/google/btree"
)

// Index 抽象索引接口，后续接入其他数据结构，只需要实现这个接口即可
type Index interface {
	// Put 向索引存储对应的数据位置信息
	Put(key []byte, pos *data.LogRecordsPos) bool
	//Get 根据索引得到对应的数据位置信息
	Get(key []byte) *data.LogRecordsPos
	//Delete 删除索引中对应的数据位置信息
	Delete(key []byte) bool
}

// Item btree需要定义的item
type Item struct {
	key []byte
	pos *data.LogRecordsPos
}

// Less btree需要定义一个排序的方法
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
