package index

//B+树索引，在磁盘上
import (
	"my_bitcask/data"
	"path/filepath"

	"go.etcd.io/bbolt"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree 创建一个新的B+树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}
	//创建索引bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}
	return &BPlusTree{
		tree: bptree,
	}
}

// Put 向索引存储对应的数据位置信息
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordsPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

// Get 根据索引得到对应的数据位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordsPos {
	var pos *data.LogRecordsPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// Delete 删除索引中对应的数据位置信息
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordsPos, bool) {
	var (
		olderIt []byte
	)
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)

		if olderIt = bucket.Get(key); len(olderIt) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	if len(olderIt) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(olderIt), true
}
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

// B+树迭代器
type bptreeIterator struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Last()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.First()
	}
}
func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.curKey, bpi.curValue = bpi.cursor.Seek(key)
}
func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Next()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.Prev()
	}
}
func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.curKey) != 0
}
func (bpi *bptreeIterator) Key() []byte {
	return bpi.curKey
}
func (bpi *bptreeIterator) Value() *data.LogRecordsPos {
	return data.DecodeLogRecordPos(bpi.curValue)
}
func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}

func (bpt *BPlusTree) Size() int { //返回索引中存了多少数据
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil

	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}
