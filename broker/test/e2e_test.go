package e2e_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"kafka-from-scratch/internal/message"
	"kafka-from-scratch/internal/peer"
	"kafka-from-scratch/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupConnection(t *testing.T) net.Conn {
	conn, err := net.Dial("tcp", "localhost:9093")
	require.NoError(t, err)
	return conn
}

func sendRequest(t *testing.T, conn net.Conn, request message.DefaultRequest) []byte {
	reqBytes, err := request.ToBytes()
	require.NoError(t, err)

	conn.SetWriteDeadline(time.Now().Add(peer.WriteTimeout))
	_, err = conn.Write(reqBytes)
	require.NoError(t, err)

	readBuf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(peer.ReadTimeout))
	n, err := conn.Read(readBuf)
	require.NoError(t, err)

	return readBuf[:n]
}

func assertExpectedResponse(t *testing.T, request message.DefaultRequest, response message.APIVersionsResponse) {
	assert.Equal(t, request.CorrelationID, response.CorrelationID)
	assert.NotNil(t, response.NumberOfAPIKeys)
	assert.Equal(t, response.ErrorCode, int16(0))

	require.Greater(t, len(response.APIVersions), 0)

	for _, api := range response.APIVersions {
		assert.NotNil(t, api.APIKey)
		assert.NotNil(t, api.MinVersion)
		assert.NotNil(t, api.MaxVersion)
	}
}

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
		conn := setupConnection(t)
		defer conn.Close()

		request := message.DefaultRequest{
			MessageSize:       0,
			RequestAPIKey:     99, // invalid API key
			RequestAPIVersion: 2020,
			CorrelationID:     12345678,
		}

		responseBytes := sendRequest(t, conn, request)
		response, err := message.NewErrorResponseMessageFromBuffer(responseBytes)
		require.NoError(t, err)
		assert.Equal(t, request.CorrelationID, response.CorrelationID)
	})

	t.Run("should return an empty response for valid api request", func(t *testing.T) {
		conn := setupConnection(t)
		defer conn.Close()

		request := message.DefaultRequest{
			MessageSize:       0,
			RequestAPIKey:     18,
			RequestAPIVersion: 4,
			CorrelationID:     12345678,
		}

		responseBytes := sendRequest(t, conn, request)
		response, err := message.NewAPIVersionsResponseFromBytes(responseBytes)
		require.NoError(t, err)

		assertExpectedResponse(t, request, *response)
	})

	t.Run("should support multiple sequential requests in the same connection", func(t *testing.T) {
		conn := setupConnection(t)
		defer conn.Close()

		for i := range 10 {
			request := message.DefaultRequest{
				MessageSize:       0,
				RequestAPIKey:     18,
				RequestAPIVersion: 4,
				CorrelationID:     int32(1000000 + i),
			}

			responseBytes := sendRequest(t, conn, request)
			response, err := message.NewAPIVersionsResponseFromBytes(responseBytes)
			require.NoError(t, err)

			assertExpectedResponse(t, request, *response)
		}
	})

	t.Run("should support multiple concurrent requests", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := range 20 {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()
				conn := setupConnection(t)
				defer conn.Close()

				request := message.DefaultRequest{
					MessageSize:       0,
					RequestAPIKey:     18,
					RequestAPIVersion: 4,
					CorrelationID:     int32(1000000 + i),
				}

				responseBytes := sendRequest(t, conn, request)
				response, err := message.NewAPIVersionsResponseFromBytes(responseBytes)
				require.NoError(t, err)

				assertExpectedResponse(t, request, *response)
			}(i)
		}

		wg.Wait()
	})

	t.Run("should close the connection after read timeout", func(t *testing.T) {
		conn := setupConnection(t)
		defer conn.Close()

		request := message.DefaultRequest{
			MessageSize:       0,
			RequestAPIKey:     18,
			RequestAPIVersion: 4,
			CorrelationID:     int32(1000000),
		}

		responseBytes := sendRequest(t, conn, request)
		response, err := message.NewAPIVersionsResponseFromBytes(responseBytes)
		require.NoError(t, err)

		assertExpectedResponse(t, request, *response)

		time.Sleep(peer.ReadTimeout + time.Second)

		readBuf := make([]byte, 1024)
		_, err = conn.Read(readBuf)
		require.ErrorIs(t, err, io.EOF)
	})
}
