package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"my_bitcask/fio"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件ID
	WriteOff  int64         // 文件写入的位置(偏移)
	IOManager fio.IOManager // io读写管理
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	//初始化IOManager管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	//return newDataFile(fileName, fileId)
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	//更新文件写入的位置
	df.WriteOff += int64(n)
	return nil
}

// ReadLogRecord 从文件中读取logRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err

	}
	var headerBytes int64 = maxLogRecordHeaderSize
	//如果offset+maxLogRecordHeaderSize大于文件大小，说明读到了文件的末尾,直接读到文件末尾就好了
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset

	}
	//读取header信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	//解析header
	header, headersize := decodeLogRecordHeader(headerBuf)
	//下面两个判断表示读到了文件的末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	//取出key，value长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headersize + keySize + valueSize
	logRecord := &LogRecord{Type: header.recordTyoe}
	//开始读取用户实际存储的kv
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headersize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	//校验数据是否正确
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headersize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}
