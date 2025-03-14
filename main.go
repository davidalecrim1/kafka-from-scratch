package main

import (
	"context"
	"log"
	"log/slog"

	"kafka-from-scratch/server"
)

func main() {
	ctx := context.Background()
	slog.SetLogLoggerLevel(slog.LevelDebug)

	s := server.NewServer(
		server.Config{
			ListenAddr: ":9092",
		},
	)

	log.Fatal(s.Start(ctx))
}
