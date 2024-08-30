package fio

import "os"

//fio专门存放关于文件IO相关代码，文件操作的接口

// FileIO 文件IO结构体
type FileIO struct {
	fd *os.File // 系统文件描述符
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// Read 从文件的给定位置读取对应的数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 将内存缓冲区数据持久化到磁盘中
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()

}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
