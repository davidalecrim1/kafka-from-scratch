package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"

	"kafka-from-scratch/internal/message"
	"kafka-from-scratch/internal/peer"
)

type Config struct {
	ListenAddr string
}

type Server struct {
	Config
	ln                net.Listener
	mu                sync.RWMutex
	peers             sync.Map
	closePeerCallback chan *peer.Peer
}

func NewServer(cfg Config) *Server {
	return &Server{
		Config:            cfg,
		closePeerCallback: make(chan *peer.Peer, 10),
	}
}

func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln

	s.acceptConnections()

	go s.watchClosedPeers(ctx)

	return nil
}

func (s *Server) acceptConnections() {
	for {
		conn, err := s.ln.Accept()
		if err != nil && errors.Is(err, net.ErrClosed) {
			slog.Info("the server listener was closed, the sever is stoping to watch new connections...")
			return
		}

		if err != nil {
			slog.Error("received an error while accepting connections", "error", err)
			continue
		}

		slog.Debug("received a new connection", "RemoteAddr", conn.RemoteAddr(), "LocalAddr", conn.LocalAddr())

		go s.handleConnections(conn)
	}
}

func (s *Server) handleConnections(conn net.Conn) {
	peer := s.CreatePeer(conn)

	go peer.Read()
	go s.handleMessages(peer)
}

func (s *Server) handleMessages(p *peer.Peer) {
	// TODO: Validate if I will have peers in the server
	//  where the connection is already closed because of the logic within peer for reads and writes

outer:
	for {
		select {
		case rawMsg, ok := <-p.WaitForMessages():
			if !ok {
				slog.Debug("the read callback channel was closed, stopped handling messages.", "RemoteAddr", p.RemoteAddr())
				break outer
			}

			request, err := message.NewDefaultMessageFromBytes(rawMsg)
			if err != nil {
				slog.Error("failed to parse received message to structured message", "error", err)
				p.Send([]byte(nil))
				continue
			}

			slog.Debug("parsed the data received to a message", "request", request)

			if request.RequestAPIVersion != 4 {
				s.sendErrorResponse(request, p)
				continue
			}

			switch request.RequestAPIKey {
			case 18:
				response, err := message.NewAPIVersionsResponse(request.CorrelationID)
				if err != nil {
					slog.Error("received an error creating the api version response", "error", err)
					continue
				}

				responseBytes, err := response.ToBytes()
				if err != nil {
					slog.Error("failed to create response, sending nothing", "error", err)
					p.Send([]byte(nil))
					continue
				}

				slog.Debug("sending a message", "message", response)
				_, err = p.Send(responseBytes)
				if err != nil {
					slog.Debug("failed to send the message", "message", response)
					continue
				}

			default:
				s.sendErrorResponse(request, p)
			}
		}
	}
}

func (s *Server) sendErrorResponse(request *message.DefaultRequest, p *peer.Peer) {
	response := message.NewErrorResponseMessage(
		request.CorrelationID,
		int16(message.ErrCodeInvalidRequestApiVersion),
	)

	bytesResp, err := response.ToBytes()
	if err != nil {
		slog.Error("failed to create response, sending nothing", "error", err)
		_, _ = p.Send([]byte(nil))
	}

	_, err = p.Send(bytesResp)
	if err != nil {
		slog.Debug("failed to send the message", "message", response)
	}
}

func (s *Server) CreatePeer(conn net.Conn) *peer.Peer {
	readCallback := make(chan []byte, 1)
	peer := peer.NewPeer(conn, readCallback, s.closePeerCallback)
	s.peers.Store(peer, struct{}{})
	return peer
}

func (s *Server) watchClosedPeers(ctx context.Context) {
outer:
	for {
		select {
		case peer, ok := <-s.closePeerCallback:
			if !ok {
				slog.Debug("the close peer callback channel was closed")
				break outer
			}

			s.peers.Delete(peer)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) Close() error {
	// TODO: Need a better solution when the server is shutdown to close all the peers in use.
	// Right now let's just close the listener
	return s.ln.Close()
}
