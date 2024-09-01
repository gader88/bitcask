package my_bitcask

import (
	"my_bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewWriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-watchbatch-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	value := utils.RandomValue(1)
	err = wb.Put(utils.GetTestKey(1), value)
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	// 未提交，没有数据
	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	//获取提交后的数据
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, value, val)

	// 事务删除数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	// 查不到数据了
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)
}
