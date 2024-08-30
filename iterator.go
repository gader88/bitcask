package my_bitcask

import (
	"bytes"
	"my_bitcask/index"
)

//提供给用户的迭代器接口

type Iterator struct {
	indexIter index.Iterator  //索引的迭代器，方便拿出key和索引
	db        *DB             //需要DB拿出对应的value
	options   IteratorOptions //迭代器配置
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(options.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   options,
	}
}

// Rewind 重新回到迭代器起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的key返回第一个大于（或小于）等于key的item
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 返回下一个item
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 判断迭代器是否有效,用于推出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前位置Key数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的value数据
func (it *Iterator) Value() ([]byte, error) { // 拿到对应的value
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器，释放资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 跳过所有不满足options中prefix的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
