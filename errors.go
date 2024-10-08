package my_bitcask

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("fail to update index")
	ErrKeyNotFound            = errors.New("key is not found")
	ErrDataFileNotFound       = errors.New("data file is not found ")
	ErrDatabaseDirIsEmpty     = errors.New("database dir is empty")
	ErrDataSizeIsInvalid      = errors.New("data size is not valid")
	ErrMergeRatioIsInvalid    = errors.New("data file merge ratio is not valid")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch number")
	ErrMergeIsProgress        = errors.New("merge is in progress, try again later")
	ErrDataBaseIsUsing        = errors.New("the database directory is used by another process")
	ErrMergeRatioUnreached    = errors.New("the merge ratio do not reach the option")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough disk space for merge")
)
