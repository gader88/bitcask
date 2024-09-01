package my_bitcask

import (
	"io"
	"my_bitcask/data"
	"os"
	"path"
	"sort"
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
			if logRecordPos != nil && logRecordPos.Offset == offset && logRecordPos.Fid == dataFile.FileID {
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
}

// getMergePath 获取merge文件的路径
// test/db
// test/db-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}
