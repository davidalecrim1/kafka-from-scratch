package e2e_test

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"

	"kafka-from-scratch/internal/message"
	"kafka-from-scratch/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E(t *testing.T) {
	ctx := context.Background()

	s := server.NewServer(server.Config{
		ListenAddr: "localhost:9093",
	})

	go func(t *testing.T) {
		err := s.Start(ctx)
		require.NoError(t, err)
	}(t)

	defer s.Close()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	t.Run("should return an error for invalid request api sent", func(t *testing.T) {
		conn, err := net.Dial("tcp", "localhost:9093")
		require.NoError(t, err)

		request := message.DefaultRequest{
			MessageSize:       0,
			RequestAPIKey:     20, // invalid API key
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

	t.Run("should return an empty response for valid api request", func(t *testing.T) {
		conn, err := net.Dial("tcp", "localhost:9093")
		require.NoError(t, err)

		request := message.DefaultRequest{
			MessageSize:       0,
			RequestAPIKey:     1,
			RequestAPIVersion: 4,
			CorrelationID:     12345678,
		}

		reqBytes, err := request.ToBytes()
		require.NoError(t, err)

		_, err = conn.Write(reqBytes)
		require.NoError(t, err)

		readBuf := make([]byte, 1024)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)

		response, err := message.NewEmptyResponse(readBuf[:n])
		require.NoError(t, err)

		assert.Equal(t, request.CorrelationID, response.CorrelationID)
		assert.NotNil(t, response.APIKey)
		assert.NotNil(t, response.APIMinNumber)
		assert.NotNil(t, response.APIMaxNumber)
		assert.NotNil(t, response.NumberOfAPIKeys)
		assert.Equal(t, response.ErrorCode, int16(0))
	})

	t.Run("should support multiple sequential requests in the same connection", func(t *testing.T) {
		conn, err := net.Dial("tcp", "localhost:9093")
		require.NoError(t, err)

		for i := range 20 {
			request := message.DefaultRequest{
				MessageSize:       0,
				RequestAPIKey:     1,
				RequestAPIVersion: 4,
				CorrelationID:     int32(1000000 + i),
			}

			reqBytes, err := request.ToBytes()
			require.NoError(t, err)

			_, err = conn.Write(reqBytes)
			require.NoError(t, err)

			readBuf := make([]byte, 1024)
			n, err := conn.Read(readBuf)
			require.NoError(t, err)

			response, err := message.NewEmptyResponse(readBuf[:n])
			require.NoError(t, err)

			assert.Equal(t, request.CorrelationID, response.CorrelationID)
			assert.NotNil(t, response.APIKey)
			assert.NotNil(t, response.APIMinNumber)
			assert.NotNil(t, response.APIMaxNumber)
			assert.NotNil(t, response.NumberOfAPIKeys)
			assert.Equal(t, response.ErrorCode, int16(0))
		}
	})

	t.Run("should support multiple concorrent requests", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := range 20 {
			wg.Add(1)

			go func() {
				defer wg.Done()
				conn, err := net.Dial("tcp", "localhost:9093")
				require.NoError(t, err)

				request := message.DefaultRequest{
					MessageSize:       0,
					RequestAPIKey:     1,
					RequestAPIVersion: 4,
					CorrelationID:     int32(1000000 + i),
				}

				reqBytes, err := request.ToBytes()
				require.NoError(t, err)

				_, err = conn.Write(reqBytes)
				require.NoError(t, err)

				readBuf := make([]byte, 1024)
				n, err := conn.Read(readBuf)
				require.NoError(t, err)

				response, err := message.NewEmptyResponse(readBuf[:n])
				require.NoError(t, err)

				assert.Equal(t, request.CorrelationID, response.CorrelationID)
				assert.NotNil(t, response.APIKey)
				assert.NotNil(t, response.APIMinNumber)
				assert.NotNil(t, response.APIMaxNumber)
				assert.NotNil(t, response.NumberOfAPIKeys)
				assert.Equal(t, response.ErrorCode, int16(0))
			}()

		}

		wg.Wait()
	})
}
