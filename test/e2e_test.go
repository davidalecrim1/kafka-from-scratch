package e2e_test

import (
	"log/slog"
	"net"
	"testing"

	"kafka-from-scratch/internal/message"
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

	t.Run("should return an error for invalid request api sent", func(t *testing.T) {
		conn, err := net.Dial("tcp", "localhost:9093")
		require.NoError(t, err)

		request := message.DefaultRequest{
			MessageSize:       0,
			RequestAPIKey:     20,
			RequestAPIVersion: 2020,
			CorrelationID:     12345678,
		}

		reqBytes, err := request.ToBytes()
		require.NoError(t, err)

		_, err = conn.Write(reqBytes)
		require.NoError(t, err)

		readBuf := make([]byte, 1024)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)

		response, err := message.NewErrorResponseMessage(readBuf[:n])
		require.NoError(t, err)

		assert.Equal(t, request.CorrelationID, response.CorrelationID)
	})
}
