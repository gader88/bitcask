package my_bitcask

import (
	"encoding/binary"
	"my_bitcask/data"
	"sync"
	"sync/atomic"
)

const (
	nonTransactionSeqNo uint64 = 0
)

// txnFinKey 标识事务完成的key
var txnFinKey = []byte("txn_fin")

//有关WritBatch 原子批量写的相关代码

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //暂存用户写的数据
}

func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPTree && !db.seqNoFileExists && !db.isInitial {
		panic("can not use write batch, seq-no file not exists")
	}
	return &WriteBatch{
		options:       options,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 将数据写入到批量写中，只缓存不提交
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:   key,
		Value: value,
	}
	return nil
}

// Delete 删除数据,将删除的数据记录下来，表示为删除
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	//数据不存在则直接返回
	LogRecordPos := wb.db.index.Get(key)
	if LogRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}
	//数据存在,暂存数据
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:   key,
		Value: nil,
		Type:  data.LogRecordDeleted,
	}
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}
	//对数据库加锁，保证事务的提交是串行化的
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1) //这行代码的作用是将 wb.db.seqNo 的值增加 1，并且这个加法操作是线程安全的，不会因为多个 goroutine 同时访问而导致竞态条件。
	//写数据到文件中
	positions := make(map[string]*data.LogRecordsPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}
	//再写一条标识数据完成的数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}
	//根据用户配置持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	//更新索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
	}
	//清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// logRecordKeyWithSeq 生成一个带有序列号的key
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo) //用于将一个无符号变长整数（uint64 类型）编码到一个字节切片中。
	encKey := make([]byte, len(key)+n)
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// parseLogRecordKeyWithSeq 解析带有序列号的key
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seq, n := binary.Uvarint(key)
	return key[n:], seq
}
