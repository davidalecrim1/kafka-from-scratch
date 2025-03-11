package main

import (
	"log"
	"log/slog"

	"kafka-from-scratch/server"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	s := server.NewServer(
		server.Config{
			ListenAddr: ":9092",
		},
	)

	log.Fatal(s.Start())
}
