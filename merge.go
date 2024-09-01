package my_bitcask

import (
	"io"
	"my_bitcask/data"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) Merge() error {
	//如果没有活跃文件说明数据库是空的，直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	//同一时间只有一个Merge操作
	if db.isMerge {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerge = true
	defer func() {
		db.isMerge = false

	}()
	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	//将活跃文件转换为老文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	//打开新的活跃文件
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	//记录一下没有被merge的文件
	nonMergeFileId := db.activeFile.FileId
	//取出所有需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	//取出文件之后即可对数据库解锁
	db.mu.Unlock()
	//对所有待merge的文件按照id从小到大排序
	sort.Slice(mergeFiles, func(i int, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()
	//如果目录存在，则表示发生过merge，删掉目录
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//重建merge目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新建的bitcask实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	//大开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	// 遍历每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)

			// 此时内存索引的数据和当前数据文件的数据是一样的，表示当前数据是有效的
			if logRecordPos != nil && logRecordPos.Offset == offset && logRecordPos.Fid == dataFile.FileId {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将位置索引写到hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}
	//对所有数据持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//写标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// getMergePath 获取merge文件的路径
// test/db
// test/db-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

// loadMergeFile 加载merge数据目录
func (db *DB) loadMergeFile() error {
	mergePath := db.getMergePath()
	// merge不存在直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	// 读取merge中所有文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识merge完成的文件是否存在
	var (
		mergeFinished  bool
		mergeFileNames []string
	)

	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
			break
		}
	}

	// merge未完成
	if !mergeFinished {
		return nil
	}

	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的标识文件
	var fileID uint32 = 0
	for ; fileID < nonMergeFileID; fileID++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileID)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 移动新的数据文件至数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

// getNonMergeFileID 获取最近没有参数merge的文件ID
func (db *DB) getNonMergeFileID(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileID), nil
}

func (db *DB) loadIndexFromHintFile() error {
	//查看hint文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//打开hint文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到索引信息
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}

	return nil
}
