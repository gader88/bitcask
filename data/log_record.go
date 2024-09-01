package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished //表示事务提交
)

// crc type keysize valuesize
// 4    1    5        5
const maxLogRecordHeaderSize = 5 + binary.MaxVarintLen32*2 // 最大的日志记录头部大小

// LogRecord 写入数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// logRecordHeader 日志记录头部信息
type logRecordHeader struct {
	crc        uint32
	recordTyoe LogRecordType
	keySize    uint32
	valueSize  uint32
}

// LogRecordsPos 数据内存索引结构，主要描述数据在磁盘的位置
type LogRecordsPos struct {
	Fid    uint32 //文件id，表示数据存储到哪个文件中
	Offset int64  //偏移，表示数据存在文件的哪个位置
}

// TransactionRecord 暂存事务相关的结构体
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordsPos
}

// crc 4 type 1 keysize 5 valuesize 5
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type
	index := 5
	// 装 keySize 和 valueSize 到headerBytes中,这两个是变长的，所以用PutVarint
	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Key)))
	index += binary.PutUvarint(header[index:], uint64(len(logRecord.Value)))
	var size = int64(index + len(logRecord.Key) + len(logRecord.Value))
	//最终的编码数组
	encBytes := make([]byte, size)
	//将header的内容拷贝过来
	copy(encBytes[:index], header[:index])
	//将key和value拷贝过来
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	//crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	//小端序
	binary.LittleEndian.PutUint32(encBytes, crc)
	return encBytes, size
}

// decodeLogRecordHeader 解码日志记录头部信息,返回的int是header长度
func DecodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 { //连crc都没有必有问题
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf),
		recordTyoe: buf[4],
	}
	var index = 5
	//解析keySize和valueSize
	keySize, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, 0
	}
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, 0

	}
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
