package redis

import (
	"encoding/binary"
	"errors"
	"my_bitcask"
	"my_bitcask/utils"
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

// -----------------------------------------------Set-----------------------------------------------
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	//构造数据部分的key
	sk := &setInternalKey{
		key:     key,
		member:  member,
		version: meta.version,
	}
	var ok bool
	if _, err = rds.db.Get(sk.encode()); err == my_bitcask.ErrKeyNotFound {
		//不存在就更新
		wb := rds.db.NewWriteBatch(my_bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true

	}
	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	//构造数据部分的key
	sk := &setInternalKey{
		key:     key,
		member:  member,
		version: meta.version,
	}
	if _, err = rds.db.Get(sk.encode()); err == my_bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// SRem 移除集合中的一个或多个成员
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	//查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	//构造数据部分的key
	sk := &setInternalKey{
		key:     key,
		member:  member,
		version: meta.version,
	}
	if _, err = rds.db.Get(sk.encode()); err == my_bitcask.ErrKeyNotFound {
		return false, nil
	}
	wb := rds.db.NewWriteBatch(my_bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// -----------------------------------------------List-----------------------------------------------
func (rds *RedisDataStructure) LPush(key []byte, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key []byte, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

// 返回当前一共有多少数据
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// isLeft == true 表示左边push，反之是右边push
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, nil
	}

	// 构造数据部分的key
	lk := listInternalKey{
		key:     key,
		version: meta.version,
	}

	// 如果是左边插入，index根据head修改
	// 当head == tail时，表示此时链表为空
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据部分
	// 原子提交
	wb := rds.db.NewWriteBatch(my_bitcask.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err := wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	lk := listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}
	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	wb := rds.db.NewWriteBatch(my_bitcask.DefaultWriteBatchOptions)
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(lk.encode())
	if err := wb.Commit(); err != nil {
		return nil, err
	}
	return element, nil
}

// -----------------------------------------------Zset-----------------------------------------------
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return false, err
	}
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	exist := true
	v, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, my_bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, my_bitcask.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		// 如果已经存在，且score相等，则不做任何修改
		if score == utils.FloatFromBytes(v) {
			return false, nil
		}
	}

	// 更新元数据和数据部分
	wb := rds.db.NewWriteBatch(my_bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	// 如果存在需要获取sk_withScore
	// 因为对应with member来说，直接更新<key+version+member,score>即可修改对应的分数
	// 而对于with score，需要删除原来得分的member，再插入新的得分的member
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(v),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, err
	}
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}
	return utils.FloatFromBytes(value), nil
}

// 找到元数据
func (rds *RedisDataStructure) findMetaData(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, my_bitcask.ErrKeyNotFound) {
		return nil, err
	}
	var meta *metadata
	var exist = true
	if errors.Is(err, my_bitcask.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// 判断数据类型是否匹配
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
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
