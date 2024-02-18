package repository

import (
	"log"
	"net"
)

// ConnPool is a simple connection pool for TCP connections
// Copiado de https://medium.com/@rozenslin/quick-approach-of-a-sized-tcp-connection-pool-in-golang-13dad814c53b

type ConnPool struct {
	// buffered channel for connection pooling
	c chan net.Conn

	// factory to create new connection
	f func() (net.Conn, error)
}

// get one idle conn from pool, if pool empty, create a new one
func (p *ConnPool) Get() (net.Conn, error) {
	select {
	case c := <-p.c:
		return c, nil

	default:
		log.Printf("Creating new connection")
		c, err := p.f()
		if c == nil || err != nil {
			return nil, err
		}

		return c, nil
	}
}

func (p *ConnPool) Put(c net.Conn) {
	select {
	case p.c <- c:

	default:
		c.Close()
	}
}

func NewPool(s int, f func() (net.Conn, error)) *ConnPool {
	return &ConnPool{
		c: make(chan net.Conn, s),
		f: f,
	}
}
