package xhttp

import (
	"io"
	"net"
	"sync"
	"time"
)

// splitConn is the net.Conn handed to upper layers. Reads come from the GET
// response body (client) or from the GET request body / uploadQueue (server);
// Writes go into the POST request body (client) or response body (server).
type splitConn struct {
	reader io.Reader
	writer io.Writer
	local  net.Addr
	remote net.Addr
	once   sync.Once
	onClose func() error
}

func (c *splitConn) Read(b []byte) (int, error)  { return c.reader.Read(b) }
func (c *splitConn) Write(b []byte) (int, error) { return c.writer.Write(b) }

func (c *splitConn) Close() error {
	var err error
	c.once.Do(func() {
		if w, ok := c.writer.(io.Closer); ok {
			_ = w.Close()
		}
		if r, ok := c.reader.(io.Closer); ok {
			_ = r.Close()
		}
		if c.onClose != nil {
			err = c.onClose()
		}
	})
	return err
}

func (c *splitConn) LocalAddr() net.Addr                { return c.local }
func (c *splitConn) RemoteAddr() net.Addr               { return c.remote }
func (c *splitConn) SetDeadline(t time.Time) error      { return nil }
func (c *splitConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *splitConn) SetWriteDeadline(t time.Time) error { return nil }
