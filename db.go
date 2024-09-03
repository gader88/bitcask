package my_bitcask

import (
	"errors"
	"fmt"
	"io"
	"my_bitcask/data"
	"my_bitcask/index"
	"my_bitcask/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// db文件主要存储一些面向用户的接口
const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB 数据库接口
type DB struct {
	mu              *sync.RWMutex
	activeFile      *data.DataFile            //当前活跃数据文件
	olderFiles      map[uint32]*data.DataFile //旧数据文件
	options         Options                   //数据库配置
	index           index.Indexer             //索引
	fileIds         []int                     //存放文件id,仅限于加载索引的时候使用
	seqNo           uint64                    //事务序列号，全局递增
	isMerge         bool                      //是否在合并数据文件,同一时间只有一个merge
	seqNoFileExists bool                      //是否存在seqNo文件
	isInitial       bool                      //是否是第一次加载
	reclaimSize     int64                     //标识有多少数据是无效的
}
type Stat struct {
	KeyNum          uint   //key总数量
	DataFileNum     uint   //数据文件数量
	ReclaimableSize int64  //可回收的数据大小,字节为单位
	DiskSize        uint64 //数据目录所占磁盘空间大小

}

// Open 打开一个新的数据库
func Open(options Options) (*DB, error) {
	//检查配置是否合法
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//判断数据目录是否存在，不存在的话创建目录
	var isInitail bool
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitail = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitail = true
	}
	//初始化数据库
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitail,
	}
	//加载merge数据目录
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}
	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	if options.IndexType != BPTree {
		//从hint中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		//再从数据文件中加载索引，
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}
	//取出当前事务序列表
	if options.IndexType == BPTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	//关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}
	//保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}
	//关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	//关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Stat 返回数据库相关的统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size:%v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Put 写入一条数据
func (db *DB) Put(key []byte, value []byte) error {
	// key为空时
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	// 更新索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordsPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)

}
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordsPos, error) {
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
	pos := &data.LogRecordsPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size)}
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
	//根据索引信息获取数据
	return db.getValueByPosition(logRecordPos)
}

// getValueByPosition 根据索引信息获取数据
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordsPos) ([]byte, error) {
	var dataFiler *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFiler = db.activeFile
	} else {
		dataFiler = db.olderFiles[logRecordPos.Fid]
	}
	// 数据文件为空
	if dataFiler == nil {
		return nil, ErrDataFileNotFound
	}

	//根据偏移量读取数据
	logRecord, _, err := dataFiler.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//判断一下数据的类型是不是删除的
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// Delete 删除一条数据
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//判断key是否存在，不存在就直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	//构造一个删除的LogRecord
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: nil,
		Type:  data.LogRecordDeleted,
	}
	//将LogRecord写入到数据文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)
	//把内存索引中对应的key删除
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil

}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("data file size is invalid")
	}

	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio")
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

	//检查是否有过merge
	hasMerge, nonMergeFileID := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		fileID, err := db.getNonMergeFileID(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileID = fileID
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordsPos) {
		var oldPos *data.LogRecordsPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key) //如果是删除的直接从索引删除
			db.reclaimSize += int64(pos.Size)
		} else { //否则保存到索引中
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}

	}
	//暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	//遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		//如果比最近未参与merge的文件id小，则已经从merge文件中加载索引
		if hasMerge && fileId < nonMergeFileID {
			continue
		}
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
			logRecordPos := &data.LogRecordsPos{Fid: fileId, Offset: offset, Size: uint32(size)}
			//解析出key和seq，判断这个数据是不是有效的
			realkey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				//非事务操作，直接更新内存索引
				updateIndex(realkey, logRecord.Type, logRecordPos)
			} else {
				//事务完成，将事务中的数据更新到内存索引
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else { //否则  说明还没到事务提交的记录，先暂存起来
					logRecord.Key = realkey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}

			}
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			offset += size
		}
		if i == len(db.fileIds)-1 { //最后一个文件，记录下当前的Write offset
			db.activeFile.WriteOff = offset
		}
	}
	//更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var i int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[i] = iterator.Key()
		i++
	}
	return keys
}

// Fold 遍历数据，对每个数据执行func，直到遍历结束或者func返回false
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil

}
