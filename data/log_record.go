package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordsPos 数据内存索引结构，主要描述数据在磁盘的位置
type LogRecordsPos struct {
	Fid    uint32 //文件id，表示数据存储到哪个文件中
	Offset int64  //偏移，表示数据存在文件的哪个位置
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
