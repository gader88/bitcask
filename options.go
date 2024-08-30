package my_bitcask

import "os"

type Options struct {
	DirPath string //数据库数据目录

	DataFileSize int64 //数据文件大小

	SyncWrites bool //是否每次写入都持久化

	IndexType IndexerType //索引类型
}

// IteratorOptions 迭代器配置
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的key
	Reverse bool   // 反向遍历，默认false是正向
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	//BytePerSync:        0,
	SyncWrites: false,
	IndexType:  Btree,
	//MMapAtStartup:      true,
	//DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
