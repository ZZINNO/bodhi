package main

import (
	"bodhi/client"
	"log"
)

func main() {
	a := client.Bodhi{}
	err := a.New("pulsar://120.238.139.21:46650", "boom", func(byte2 []byte) {
		log.Println(string(byte2))
	})
	if err != nil {
		log.Println(err)
	}
	select {}
}
