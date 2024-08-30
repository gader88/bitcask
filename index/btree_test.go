package index

import (
	"my_bitcask/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordsPos{1, 100})
	assert.True(t, res1)
	res2 := bt.Put(nil, &data.LogRecordsPos{1, 200})
	assert.True(t, res2)
}
