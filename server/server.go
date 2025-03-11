package server

import (
	"log/slog"
	"net"
	"sync"

	peer "kafka-from-scratch/internal"
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
		if err != nil {
			slog.Error("received an error while accepting connections", "error", err)
			continue
		}

		go s.handleConnections(conn)
	}
}

func (s *Server) handleConnections(conn net.Conn) {
	peer := s.CreatePeer(conn)
	// TODO: add the read properly

	peer.Send([]byte("Hi"))
	peer.Close()
}

func (s *Server) CreatePeer(conn net.Conn) *peer.Peer {
	peer := peer.NewPeer(conn)
	s.peers.Store(peer, struct{}{})
	return peer
}

func (s *Server) Close() error {
	s.peers.Range(func(key, value any) bool {
		peer, ok := key.(*peer.Peer)
		if !ok {
			return true // continues to the next
		}

		peer.Close()
		s.peers.Delete(key)
		return true
	})

	return s.ln.Close()
}
