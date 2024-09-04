package benchmark

import (
	"math/rand"
	"my_bitcask"
	"my_bitcask/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var db *my_bitcask.DB

func init() {
	// 初始化用户基准测试的存储引擎
	opt := my_bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-benchmark")
	opt.DirPath = dir

	var err error
	db, err = my_bitcask.Open(opt)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	// 开始计时
	b.ResetTimer()
	// 打印内存分配
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	rand.Seed(int64(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(i))
		if err != nil && err != my_bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Del(b *testing.B) {
	rand.Seed(int64(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(i))
		if err != nil && err != my_bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
