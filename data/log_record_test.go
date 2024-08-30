package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	//正常情况
	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(res1)
	t.Log(n1)
	//value为空的情况

	//Deleted情况
}

func TestDecodeLogRecordHeader(t *testing.T) {
	//正常情况
	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(res1)
	t.Log(n1)

	dec1, n2 := DecodeLogRecordHeader(res1)
	assert.NotNil(t, dec1)
	assert.Equal(t, n1, n2)
	//assert.Equal(t, dec1.keySize, uint32(5))

	//value为空的情况

	//Deleted情况
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	headerbuf := []byte{104, 82, 240, 150, 0, 8, 20}
	crc := getLogRecordCRC(rec1, headerbuf[crc32.Size:])
	assert.Equal(t, crc, uint32(2532332136))

	rec1 = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	headerbuf = []byte{9, 252, 88, 14, 0, 8, 0}
	crc = getLogRecordCRC(rec1, headerbuf[crc32.Size:])
	assert.Equal(t, crc, uint32(240712713))
}
