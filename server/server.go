package server

import (
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
	ln    net.Listener
	mu    sync.RWMutex
	peers sync.Map
}

func NewServer(cfg Config) *Server {
	return &Server{
		Config: cfg,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln

	s.acceptConnections()
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
				s.ClosePeer(p)
				return
			}

			// TODO: Actually validate the API request, now its just sending the error response
			response := message.ErrorResponseMessage{
				MessageSize:   0,
				CorrelationID: request.CorrelationID,
				ErrorCode:     35,
			}

			bytesResp, err := response.ToBytes()
			if err != nil {
				slog.Error("failed to create response, sending nothing", "error", err)
				p.Send([]byte(nil))
				s.ClosePeer(p)
				return
			}

			p.Send(bytesResp)
			s.ClosePeer(p)
		}
	}
}

func (s *Server) CreatePeer(conn net.Conn) *peer.Peer {
	readCallback := make(chan []byte, 1)
	peer := peer.NewPeer(conn, readCallback)
	s.peers.Store(peer, struct{}{})
	return peer
}

func (s *Server) ClosePeer(p *peer.Peer) {
	if _, ok := s.peers.Load(p); !ok {
		slog.Debug("the peer was already closed, doing nothing...")
		return
	}

	go p.Close()
	s.peers.Delete(p)
}

func (s *Server) Close() error {
	s.peers.Range(func(key, value any) bool {
		p, ok := key.(*peer.Peer)
		if !ok {
			return true // continues to the next
		}

		s.ClosePeer(p)
		return true
	})

	return s.ln.Close()
}
