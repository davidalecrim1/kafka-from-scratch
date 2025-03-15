package peer

import (
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

var ReadTimeout = time.Second * 5

type Peer struct {
	conn              net.Conn
	wg                sync.WaitGroup
	readCallback      chan []byte
	closePeerCallback chan *Peer
}

func NewPeer(conn net.Conn, readCallback chan []byte, closePeerCallback chan *Peer) *Peer {
	return &Peer{
		conn:              conn,
		readCallback:      readCallback,
		closePeerCallback: closePeerCallback,
	}
}

func (p *Peer) Send(data []byte) (int, error) {
	defer p.wg.Done()

	slog.Debug("sending a message", slog.String("messageBytes", hex.EncodeToString(data)))
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
		p.conn.SetReadDeadline(time.Now().Add(ReadTimeout))

		n, err := p.conn.Read(buf)
		if err != nil && err == io.EOF {
			slog.Info("reached the EOF of the current connection, stoping the reads", "remoteAddr", p.conn.RemoteAddr())
			return
		}

		if err != nil && errors.Is(err, net.ErrClosed) {
			slog.Info("the connection was closed, stoping the reads", "RemoteAddr", p.conn.RemoteAddr())
			go p.Close()
			return
		}

		if err, ok := err.(net.Error); ok && err.Timeout() {
			slog.Info("the read connection timeout, stoping the reads", "RemoteAddr", p.conn.RemoteAddr())
			go p.Close()
			return
		}

		if err != nil {
			slog.Debug("unexpected error while reading", "error", err)
			return
		}

		p.wg.Add(1)

		// Avois race conditions given that if the []byte from buf is sent,
		// only the slice header is copied, not the underlying array.
		sentData := make([]byte, n)
		copy(sentData, buf[:n])

		p.readCallback <- sentData
	}
}

func (p *Peer) WaitForMessages() chan []byte {
	return p.readCallback
}

func (p *Peer) RemoteAddr() net.Addr {
	return p.conn.RemoteAddr()
}
