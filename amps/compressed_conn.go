package amps

import (
	"compress/zlib"
	"net"
	"sync"
	"time"
)

type compressedNetConn struct {
	conn       net.Conn
	readCloser interface {
		Read([]byte) (int, error)
		Close() error
	}
	writer *zlib.Writer

	readLock  sync.Mutex
	writeLock sync.Mutex
	closeLock sync.Mutex
	closed    bool
}

func newCompressedNetConn(conn net.Conn) *compressedNetConn {
	if conn == nil {
		return nil
	}
	return &compressedNetConn{
		conn:   conn,
		writer: zlib.NewWriter(conn),
	}
}

func (connection *compressedNetConn) ensureReader() (interface {
	Read([]byte) (int, error)
	Close() error
}, error) {
	if connection.readCloser != nil {
		return connection.readCloser, nil
	}

	reader, err := zlib.NewReader(connection.conn)
	if err != nil {
		return nil, err
	}
	connection.readCloser = reader
	return connection.readCloser, nil
}

func (connection *compressedNetConn) Read(buffer []byte) (int, error) {
	connection.readLock.Lock()
	defer connection.readLock.Unlock()

	reader, err := connection.ensureReader()
	if err != nil {
		return 0, err
	}
	return reader.Read(buffer)
}

func (connection *compressedNetConn) Write(buffer []byte) (int, error) {
	connection.writeLock.Lock()
	defer connection.writeLock.Unlock()

	written, err := connection.writer.Write(buffer)
	if err != nil {
		return written, err
	}
	if err := connection.writer.Flush(); err != nil {
		return written, err
	}
	return written, nil
}

func (connection *compressedNetConn) Close() error {
	connection.closeLock.Lock()
	if connection.closed {
		connection.closeLock.Unlock()
		return nil
	}
	connection.closed = true
	connection.closeLock.Unlock()

	connection.writeLock.Lock()
	var writerErr error
	if connection.writer != nil {
		writerErr = connection.writer.Close()
	}
	connection.writeLock.Unlock()

	var closeErr = connection.conn.Close()

	connection.readLock.Lock()
	var readerErr error
	if connection.readCloser != nil {
		readerErr = connection.readCloser.Close()
	}
	connection.readLock.Unlock()

	if writerErr != nil {
		return writerErr
	}
	if readerErr != nil {
		return readerErr
	}
	return closeErr
}

func (connection *compressedNetConn) LocalAddr() net.Addr {
	return connection.conn.LocalAddr()
}

func (connection *compressedNetConn) RemoteAddr() net.Addr {
	return connection.conn.RemoteAddr()
}

func (connection *compressedNetConn) SetDeadline(deadline time.Time) error {
	return connection.conn.SetDeadline(deadline)
}

func (connection *compressedNetConn) SetReadDeadline(deadline time.Time) error {
	return connection.conn.SetReadDeadline(deadline)
}

func (connection *compressedNetConn) SetWriteDeadline(deadline time.Time) error {
	return connection.conn.SetWriteDeadline(deadline)
}

func (connection *compressedNetConn) NetConn() net.Conn {
	return connection.conn
}
