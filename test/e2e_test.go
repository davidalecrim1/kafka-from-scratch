package e2e_test

import (
	"log/slog"
	"net"
	"testing"

	"kafka-from-scratch/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E(t *testing.T) {
	s := server.NewServer(server.Config{
		ListenAddr: "localhost:9093",
	})

	go func(t *testing.T) {
		err := s.Start()
		require.NoError(t, err)
	}(t)

	defer s.Close()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	t.Run("should accept a connection in the server and return something", func(t *testing.T) {
		conn, err := net.Dial("tcp", "localhost:9093")
		require.NoError(t, err)

		readBuf := make([]byte, 1024)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, n, 1)

		slog.Debug("received data", "message", string(readBuf[:n]))
	})
}
