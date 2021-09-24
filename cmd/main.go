package main

import (
	"Bitcask"
	"log"
	"os"
)

func main() {
	os.RemoveAll("Storage")
	bc, err := Bitcask.Open("Storage", nil)
	if err != nil {
		log.Fatalln(err)
	}
	k1 := []byte("key1")
	v1 := []byte("val1")

	k2 := []byte("key2")
	v2 := []byte("val2")

	bc.Put(k1, v1)
	bc.Put(k2, v2)

	v1, err = bc.Get(k1)
	v2, err = bc.Get(k2)
	log.Printf("key : %s, value : %s", string(k1), string(v1))
	log.Printf("key : %s, value : %s", string(k2), string(v2))

	bc.Put(k1, []byte("another v1"))
	bc.Del(k2)
	v1, err = bc.Get(k1)
	v2, err = bc.Get(k2)
	log.Printf("key : %s, value : %s", string(k1), string(v1))
	log.Printf("key : %s, value : %s", string(k2), string(v2))

	bc.Close()

	bc, err = Bitcask.Open("Storage", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer bc.Close()

	v1, err = bc.Get(k1)
	v2, err = bc.Get(k2)
	log.Printf("key : %s, value : %s", string(k1), string(v1))
	log.Printf("key : %s, value : %s", string(k2), string(v2))
}
