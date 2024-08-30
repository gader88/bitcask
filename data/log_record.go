package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
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

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecordHeader 解码日志记录头部信息,返回的int是header长度
func decodeLogRecordHeader(headerBuf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	return 0
}
