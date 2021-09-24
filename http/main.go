package main

import (
	"Bitcask"
	"context"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"time"
)

var (
	addr        string
	storagePath string
	merged      bool
	interval    int64
	maxSize     uint64
	bc          *Bitcask.BitCask
)

func main() {
	flag.StringVar(&addr, "addr", "127.0.0.1:8080", "bitcask http listen addr")
	flag.StringVar(&storagePath, "s", "Storage", "data storage path")
	flag.BoolVar(&merged, "m", true, "true: open file merge; false: not open file merge ")
	flag.Int64Var(&interval, "t", 3600, "interval for file merging")
	flag.Uint64Var(&maxSize, "ms", 1<<32, "single data file maxsize")
	flag.Parse()

	opt := &Bitcask.Options{
		MaxFileSize: maxSize,
	}
	var err error
	bc, err = Bitcask.Open(storagePath, opt)

	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := recover(); err != nil {
			log.Fatalln(err)
			debug.PrintStack()
		}
	}()
	if merged {
		mergeWorker := Bitcask.NewMerge(bc, interval)
		mergeWorker.Start()
		defer mergeWorker.Stop()
	}

	r := mux.NewRouter()
	r.HandleFunc("/{key}", Get).Methods("GET")
	r.HandleFunc("/{key}", Del).Methods("DELETE")
	r.HandleFunc("/{key}", Put).Methods("POST")
	log.Println("Bitcask listen at : ", addr)

	s := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		if err := s.ListenAndServe(); err != nil {
			log.Fatalln(err)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("Shutdown Server ...")
	bc.Close()
	log.Println("Close the db ...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}

	log.Println("Server exiting")
}

func Put(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	key := vars["key"]
	if len(key) <= 0 {
		writer.Write([]byte(fmt.Sprintf("Key : %s is invalid", key)))
		return
	}
	value, err := ioutil.ReadAll(request.Body)
	if err != nil {
		log.Fatalln(err)
		writer.WriteHeader(500)
		return
	}
	bc.Put([]byte(key), value)
	writer.Write([]byte("Success!"))
	writer.WriteHeader(200)
}

func Del(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	key := vars["key"]
	if len(key) <= 0 {
		writer.Write([]byte(fmt.Sprintf("Key : %s is invalid", key)))
		return
	}
	err := bc.Del([]byte(key))
	if err != nil && err != Bitcask.KeyNotFoundErr {
		writer.Write([]byte(err.Error()))
		writer.WriteHeader(500)
		return
	}
	if err == Bitcask.KeyNotFoundErr {
		writer.Write([]byte(err.Error()))
		writer.WriteHeader(404)
		return
	}
	writer.Write([]byte("Success"))
	writer.WriteHeader(200)
}

func Get(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	key := vars["key"]
	if len(key) <= 0 {
		writer.Write([]byte(fmt.Sprintf("Key : %s is invalid", key)))
		return
	}
	value, err := bc.Get([]byte(key))
	if err != nil && err != Bitcask.KeyNotFoundErr {
		writer.Write([]byte(err.Error()))
		writer.WriteHeader(500)
		return
	}
	if err == Bitcask.KeyNotFoundErr {
		writer.Write([]byte(err.Error()))
		writer.WriteHeader(404)
		return
	}
	writer.Write(value)
	writer.WriteHeader(200)
}
