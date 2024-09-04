package redis

import (
	"encoding/binary"
	"errors"
	"my_bitcask"
	"time"
)

// RedisDataStructure redis数据结构
type RedisDataStructure struct {
	db *my_bitcask.DB
}

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

func NewRedisDataStructure(options my_bitcask.Options) (*RedisDataStructure, error) {
	db, err := my_bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

// -----------------------------------------------String-----------------------------------------------
// Set 设置key-value
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	//编码value：type+expire+payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)
	//调用存储接口写入
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	//解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && time.Now().UnixNano() >= expire {
		return nil, nil
	}
	return encValue[index:], nil
}

// -----------------------------------------------Hash-----------------------------------------------

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) { //不存在返回true，存在返回false
	//查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	//构造Hash数据部分的key
	hk := &hashInternalKey{key: key, field: field, version: meta.version}
	encKey := hk.encode()
	//查找是否存在
	var exist bool
	if _, err = rds.db.Get(encKey); err == my_bitcask.ErrKeyNotFound {
		exist = false
	}
	wb := rds.db.NewWriteBatch(my_bitcask.DefaultWriteBatchOptions)
	//不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	//构造Hash数据部分的key
	hk := &hashInternalKey{key: key, field: field, version: meta.version}
	encKey := hk.encode()
	//查找是否存在
	return rds.db.Get(encKey)
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	//构造Hash数据部分的key
	hk := &hashInternalKey{key: key, field: field, version: meta.version}
	encKey := hk.encode()
	//查找是否存在
	if _, err = rds.db.Get(encKey); err == my_bitcask.ErrKeyNotFound {
		return false, nil
	}
	wb := rds.db.NewWriteBatch(my_bitcask.DefaultWriteBatchOptions)
	//存在则更新元数据
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(encKey)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != my_bitcask.ErrKeyNotFound {
		return nil, err
	}
	var meta *metadata
	var exist bool
	if err == my_bitcask.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		//判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}

	}
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

type hashInternalKey struct {
	key     []byte
	field   []byte
	version int64
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8
	copy(buf[index:], hk.field)
	return buf
}
