package data

import "my_bitcask/fio"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件ID
	WriteOff  int64         // 文件写入的位置(偏移)
	IOManager fio.IOManager // io读写管理
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	//fileName := GetDataFileName(dirPath, fileId)
	//return newDataFile(fileName, fileId)
	return nil, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
