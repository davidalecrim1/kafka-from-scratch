package peer

import (
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
)

type Peer struct {
	conn         net.Conn
	wg           sync.WaitGroup
	readCallback chan []byte
}

func NewPeer(conn net.Conn, readCallback chan []byte) *Peer {
	return &Peer{
		conn:         conn,
		readCallback: readCallback,
	}
}

func (p *Peer) Send(data []byte) (int, error) {
	defer p.wg.Done()

	slog.Debug("sending a message", "messageString", string(data), slog.String("messageBytes", hex.EncodeToString(data)))
	return p.conn.Write(data)
}

func (p *Peer) Close() error {
	p.wg.Wait()
	slog.Debug("closing the peer", "RemoteAddr", p.conn.RemoteAddr())
	close(p.readCallback)
	return p.conn.Close()
}

func (p *Peer) Read() {
	buf := make([]byte, 1024)

	for {
		n, err := p.conn.Read(buf)
		if err != nil && err == io.EOF {
			slog.Info("reached the EOF of the current connection, stoping the reads", "remoteAddr", p.conn.RemoteAddr())
			return
		}

		if err != nil && errors.Is(err, net.ErrClosed) {
			slog.Info("the connection was closed, stoping the reads", "RemoteAddr", p.conn.RemoteAddr())
			return
		}

		if err != nil {
			slog.Debug("unexpected error while reading", "error", err)
			return
		}

		p.wg.Add(1)
		p.readCallback <- buf[:n]
	}
}

func (p *Peer) WaitForMessages() chan []byte {
	return p.readCallback
}

func (p *Peer) RemoteAddr() net.Addr {
	return p.conn.RemoteAddr()
}
