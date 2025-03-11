package main

import (
	"log"

	"kafka-from-scratch/server"
)

func main() {
	s := server.NewServer(
		server.Config{
			ListenAddr: ":9092",
		},
	)

	log.Fatal(s.Start())
}
