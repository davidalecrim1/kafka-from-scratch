package peer

import (
	"encoding/hex"
	"log/slog"
	"net"
)

type Peer struct {
	conn net.Conn
}

func NewPeer(conn net.Conn) *Peer {
	return &Peer{
		conn: conn,
	}
}

func (p *Peer) Send(data []byte) (int, error) {
	slog.Debug("sending a message", "messageString", string(data), slog.String("messageBytes", hex.EncodeToString(data)))
	return p.conn.Write(data)
}

func (p *Peer) Close() error {
	return p.conn.Close()
}
