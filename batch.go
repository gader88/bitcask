package my_bitcask

import (
	"my_bitcask/data"
	"sync"
)

//有关WritBatch 原子批量写的相关代码

type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //暂存用户写的数据
}
