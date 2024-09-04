package main

import (
	"fmt"
	my_bitcask "my_bitcask"
)

func main() {
	opts := my_bitcask.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := my_bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))

	db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	val, err = db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	//fmt.Println(string(val))
}
