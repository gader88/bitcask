package redis

import (
	"encoding/binary"
	"math"
	"my_bitcask/utils"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 // 数据大小
	head     uint64 // 数据头部 list专用
	tail     uint64 // 数据尾部 list专用
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	index := 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutUvarint(buf[index:], uint64(md.size))
	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Uvarint(buf[index:])
	index += n
	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	field   []byte
	version int64
}

type setInternalKey struct {
	key     []byte
	member  []byte
	version int64
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4) // 还要额外四个字节存储member的长度
	var index = 0
	// key
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	//member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))
	return buf
}

type listInternalKey struct {
	key     []byte
	index   uint64
	version int64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)
	var index = 0
	// key
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// index
	binary.LittleEndian.PutUint64(buf[index:index+8], lk.index)
	index += 8

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	return buf
}

type zsetInternalKey struct {
	key     []byte
	member  []byte
	score   float64
	version int64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	// key+version+member
	buf := make([]byte, len(zk.key)+8+len(zk.member))

	// key
	index := 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	// key+version+score+member+member_size
	// 用于根据score的分数范围，查找对应的member
	buf := make([]byte, len(zk.key)+8+len(scoreBuf)+len(zk.member)+4)

	// key
	index := 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member_size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))
	return buf
}
