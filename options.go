package my_bitcask

import "os"

type Options struct {
	DirPath string //数据库数据目录

	DataFileSize int64 //数据文件大小

	SyncWrites bool //是否每次写入都持久化

	BytePerSync int64 //累计多少字节后持久化

	IndexType IndexerType //索引类型

	MMapAtStartup bool //是否在启动时mmap数据文件

	DataFileMergeRatio float32 //数据文件合并比例
}

// IteratorOptions 迭代器配置
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的key
	Reverse bool   // 反向遍历，默认false是正向
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	//一个批次中最大数据量
	MaxBatchNum uint
	//提交事务时是否sync持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
	BPTree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	BytePerSync:        0,
	SyncWrites:         false,
	IndexType:          Btree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
