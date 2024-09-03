package http

import (
	"encoding/json"
	"fmt"
	"log"
	"my_bitcask"
	"net/http"
	"os"
)

var db *my_bitcask.DB

func init() {
	//初始化DB实例
	var err error
	options := my_bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = my_bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db :%v", err))
	}

}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	//解析数据
	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, "failed to decode request body", http.StatusBadRequest)
	}
	//拿到数据，遍历数据
	for key, value := range data {
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db:%v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	//解析数据
	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != my_bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value from db:%v\n", err)
		return
	}
	writer.Header().Set("content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != my_bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db: %v\n", err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")

}
func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 对传递的参数进行解析，传递是getUrl
	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var res []string
	for _, key := range keys {
		res = append(res, string(key))
	}
	_ = json.NewEncoder(writer).Encode(res)
}
func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 对传递的参数进行解析，传递是getUrl
	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/list", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)
	http.ListenAndServe("localhost:8080", nil)

}
