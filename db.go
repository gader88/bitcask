package my_bitcask

import (
	"errors"
	"io"
	"my_bitcask/data"
	"my_bitcask/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

//db文件主要存储一些面向用户的接口

// DB 数据库接口
type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile            //当前活跃数据文件
	olderFiles map[uint32]*data.DataFile //旧数据文件
	options    Options                   //数据库配置
	index      index.Indexer             //索引
	fileIds    []int                     //存放文件id,仅限于加载索引的时候使用
}

// Open 打开一个新的数据库
func Open(options Options) (*DB, error) {
	//检查配置是否合法
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//判断数据目录是否存在，不存在的话创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//初始化数据库
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}
	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	//从数据文件中加载索引
	return db, nil
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
	logRecord, _, err := dataFailer.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//判断一下数据的类型是不是删除的
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("data file size is invalid")
	}
	return nil
}

// loadDataFiles 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	//根据配置项读取目录下的所有文件
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	//存放文件id
	var fileIds []int
	//遍历目录下的文件,找到所有以.data结尾的文件
	//遍历文件，初始化数据文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//将文件名称分割
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			//数据目录有可能损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	//对文件id进行排序
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//遍历文件id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { //说明遍历到了最新的文件，加入到活跃数据文件中
			db.activeFile = dataFile
		} else { //旧文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 从数据文件中加载索引,遍历所有文件记录，添加到索引
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}
	//遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		//拿到每个文件
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0
		//读取文件中的记录,添加到索引
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF { //这个错误代表读到文件尾部
					break
				}
				return err
			}
			logRecordPos := &data.LogRecordsPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key) //如果是删除的直接从索引删除
			} else { //否则保存到索引中
				db.index.Put(logRecord.Key, logRecordPos)
			}
			offset += size
		}
		if i == len(db.fileIds)-1 { //最后一个文件，记录下当前的Write offset
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}
