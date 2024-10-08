package index

import (
	"bytes"
	"my_bitcask/data"

	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续接入其他数据结构，只需要实现这个接口即可
type Indexer interface {
	// Put 向索引存储对应的数据位置信息
	Put(key []byte, pos *data.LogRecordsPos) *data.LogRecordsPos
	//Get 根据索引得到对应的数据位置信息
	Get(key []byte) *data.LogRecordsPos
	//Delete 删除索引中对应的数据位置信息
	Delete(key []byte) (*data.LogRecordsPos, bool)
	Iterator(reverse bool) Iterator //Iterator 迭代器索引
	Size() int                      //返回索引中存了多少数据
	Close() error                   //关闭索引
}

// Iterator 定义一个通用索引迭代器
type Iterator interface {
	Rewind()                    // 重新回到迭代器起点
	Seek(key []byte)            // 找到第一个大于等于key的位置，从这个位置向后遍历
	Next()                      // 下一个key
	Valid() bool                // 是否遍历完所有的key
	Key() []byte                // 当前位置的key
	Value() *data.LogRecordsPos // 当前位置的value
	Close()                     // 关闭迭代器释放资源
}

type IndexType = int8 //定义索引类型

const (
	// BTreeType btree索引
	Btree IndexType = iota + 1

	//ART自适应基数树索引
	ART
	BPTree
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}

	return nil
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
