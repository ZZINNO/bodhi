package main

import (
	"log"
)

func main() {
	a := Bodhi{}
	err := a.New("pulsar://120.238.139.21:46650", "boom", func(byte2 []byte) {

		//a.SendReply()
		log.Println(string(byte2))
	})
	if err != nil {
		log.Println(err)
	}
	select {}
}
