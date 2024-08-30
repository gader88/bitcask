package my_bitcask

type Options struct {
	DirPath string //数据库数据目录

	DataFileSize int64 //数据文件大小

	SyncWrites bool //是否每次写入都持久化
}
