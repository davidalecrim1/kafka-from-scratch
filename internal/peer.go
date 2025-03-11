package peer

import "net"

type Peer struct {
	conn net.Conn
}

func NewPeer(conn net.Conn) *Peer {
	return &Peer{
		conn: conn,
	}
}

func (p *Peer) Send(data []byte) (int, error) {
	return p.conn.Write(data)
}

func (p *Peer) Close() error {
	return p.conn.Close()
}
