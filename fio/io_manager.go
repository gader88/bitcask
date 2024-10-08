package fio

const DataFilePerm = 0644 //DataFilePerm 是一个常量，用于指定文件的权限。在你的代码中，它被定义为 0644

// IOManager 抽象IO管理接口，可以接入不同的IO类型
type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 将内存缓冲区数据持久化到磁盘中
	Sync() error

	// Close 关闭文件
	Close() error

	// bSize 获取到对应文件的大小
	Size() (int64, error)
}

// 初始化IOManager
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
