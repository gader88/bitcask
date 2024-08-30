package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/temp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/temp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	_, err = fio.Read(nil, 0)
	assert.NotNil(t, err)
}
