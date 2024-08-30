package my_bitcask

import (
	"my_bitcask/data"
	"my_bitcask/index"
	"sync"
)

//db文件主要存储一些面向用户的接口

// DB 数据库接口
type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile            //当前活跃数据文件
	olderFiles map[uint32]*data.DataFile //旧数据文件
	options    *Options                  //数据库配置
	index      index.Index               //索引
}

// Put 写入一条数据
func (db *DB) Put(key []byte, value []byte) error {
	// key为空时
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 更新索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordsPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	//判断当前活跃文件是否存在，如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	//对写入的数据进行编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	//如果当前活跃文件的大小超过了阈值，则需要切换到新的数据文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//先将当前活跃文件持久化
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//将当前活跃文件移动到旧文件中
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		//设置新的活跃文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	//根据配置决定是否每次持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	//构造一个内存索引信息返回数据
	pos := &data.LogRecordsPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// setActiveFile 设置当前活跃文件
// 对db的共享数据结构的更改必须加锁
func (db *DB) setActiveFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileId + 1
	}
	//打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// Get 根据key获取对应的数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	//判断key是否为空
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	//从内存中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	//如果key不在内存
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	//根据索引信息从数据文件中读取数据
	var dataFailer *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFailer = db.activeFile
	} else {
		dataFailer = db.olderFiles[logRecordPos.Fid]
	}
	// 数据文件为空
	if dataFailer == nil {
		return nil, ErrDataFileNotFound
	}

	//根据偏移量读取数据
	logRecord, err := dataFailer.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//判断一下数据的类型是不是删除的
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}
