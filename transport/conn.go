package transport

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/request"
	. "scylla-go-driver/frame/response"
)

// TODO on send and recv i/o error we shall reset the connection
// TODO request coelasting if there is more items in requestCh than we can send them together, we can check channel length, we need a write buffer

type response struct {
	frame.Header
	frame.Response
	Err error
}

type responseHandler chan response

type request struct {
	frame.Request
	StreamID        frame.StreamID
	Compress        bool
	Tracing         bool
	ResponseHandler responseHandler
}

type connWriter struct {
	conn      io.Writer
	buf       frame.Buffer
	requestCh chan request
}

func (c *connWriter) submit(r request) {
	c.requestCh <- r
}

func (c *connWriter) loop() {
	runtime.LockOSThread()

	for {
		r, ok := <-c.requestCh
		if !ok {
			return
		}

		if err := c.send(r); err != nil {
			r.ResponseHandler <- response{Err: fmt.Errorf("send: %w", err)}
		}
	}
}

func (c *connWriter) send(r request) error {
	c.buf.Reset()

	// Dump request with header to buffer
	h := frame.Header{
		Version:  frame.CQLv4,
		StreamID: r.StreamID,
		OpCode:   r.OpCode(),
	}
	h.WriteTo(&c.buf)
	r.WriteTo(&c.buf)

	// Update length in header
	b := c.buf.Bytes()
	l := uint32(len(b) - frame.HeaderSize)
	binary.BigEndian.PutUint32(b[5:9], l)

	// Send
	if _, err := frame.CopyBuffer(&c.buf, c.conn); err != nil {
		return err
	}

	return nil
}

type connReader struct {
	conn *bufio.Reader
	buf  frame.Buffer
	bufw io.Writer

	h map[frame.StreamID]responseHandler
	s streamIDAllocator
	// mu guards h and s.
	mu sync.Mutex
}

func (c *connReader) setHandler(h responseHandler) (frame.StreamID, error) {
	c.mu.Lock()
	streamID, err := c.s.Alloc()
	if err != nil {
		c.mu.Unlock()
		return 0, fmt.Errorf("stream ID alloc: %w", err)
	}

	c.h[streamID] = h
	c.mu.Unlock()
	return streamID, err
}

func (c *connReader) freeHandler(streamID frame.StreamID) {
	c.mu.Lock()
	c.s.Free(streamID)
	delete(c.h, streamID)
	c.mu.Unlock()
}

func (c *connReader) handler(streamID frame.StreamID) responseHandler {
	c.mu.Lock()
	h := c.h[streamID]
	c.mu.Unlock()
	return h
}

func (c *connReader) loop() {
	runtime.LockOSThread()

	c.bufw = frame.BufferWriter(&c.buf)
	for {
		resp := c.recv()
		if h := c.handler(resp.StreamID); h != nil {
			h <- resp
		} else {
			// FIXME gracefully handle recv error
			log.Fatalf("recv error: %+v, %+v", resp.Header, resp.Response)
		}
	}
}

func (c *connReader) recv() response {
	c.buf.Reset()

	var r response

	// Read header
	if _, err := io.CopyN(c.bufw, c.conn, frame.HeaderSize); err != nil {
		r.Err = fmt.Errorf("read header: %w", err)
		return r
	}
	r.Header = frame.ParseHeader(&c.buf)
	if err := c.buf.Error(); err != nil {
		r.Err = fmt.Errorf("parse header: %w", err)
		return r
	}

	// Read body
	if _, err := io.CopyN(c.bufw, c.conn, int64(r.Header.Length)); err != nil {
		r.Err = fmt.Errorf("read body: %w", err)
		return r
	}
	r.Response = c.parse(r.Header.OpCode)
	if err := c.buf.Error(); err != nil {
		r.Err = fmt.Errorf("parse body: %w", err)
		return r
	}

	return r
}

func (c *connReader) parse(op frame.OpCode) frame.Response {
	// TODO add all responses
	switch op {
	case frame.OpError:
		return ParseError(&c.buf)
	case frame.OpReady:
		return ParseReady(&c.buf)
	case frame.OpResult:
		return ParseResult(&c.buf)
	case frame.OpSupported:
		return ParseSupported(&c.buf)
	default:
		log.Fatalf("not supported %d", op)
		return nil
	}
}

type Conn struct {
	conn net.Conn
	w    connWriter
	r    connReader
}

type ConnConfig struct {
	TCPNoDelay         bool
	Timeout            time.Duration
	DefaultConsistency frame.Consistency
}

const (
	requestChanSize = 1024
	ioBufferSize    = 8192
)

// OpenShardConn opens connection mapped to a specific shard on scylla node.
func OpenShardConn(addr string, si ShardInfo, cfg ConnConfig) (*Conn, error) { // nolint:unused // This will be used.
	it := ShardPortIterator(si)
	maxTries := (maxPort-minPort+1)/int(si.NrShards) + 1
	for i := 0; i < maxTries; i++ {
		if conn, err := OpenLocalPortConn(addr, it(), cfg); err == nil {
			return conn, nil
		}
	}

	return nil, fmt.Errorf("failed to open connection on shard port: all local ports are busy")
}

// OpenLocalPortConn opens connection on a given local port.
func OpenLocalPortConn(addr string, localPort uint16, cfg ConnConfig) (*Conn, error) {
	// Not sure about local IP address. Empty IP and 172.19.0.1 works fine during tests but localhost does not.
	// The problem is that when using localhost as IP connections are not mapped for appropriate shards
	// even when using shard aware policy.
	localAddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(int(localPort)))
	if err != nil {
		return nil, fmt.Errorf("resolving local TCP address: %w", err)
	}

	return OpenConn(addr, localAddr, cfg)
}

// OpenConn opens connection with specific local address.
// In case lAddr is nil, random local address is chosen.
func OpenConn(addr string, localAddr *net.TCPAddr, cfg ConnConfig) (*Conn, error) {
	d := net.Dialer{
		Timeout:   cfg.Timeout,
		LocalAddr: localAddr,
	}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dialing TCP address %s: %w", addr, err)
	}

	tcpConn := conn.(*net.TCPConn)
	if err = tcpConn.SetNoDelay(cfg.TCPNoDelay); err != nil {
		return nil, fmt.Errorf("setting TCP no delay option: %w", err)
	}

	return WrapConn(tcpConn)
}

// WrapConn transforms tcp connection to a working Scylla connection with given StreamID allocator.
// If returned error is not nil, connection is not valid - it can not be used and must be closed.
func WrapConn(conn net.Conn) (*Conn, error) {
	c := &Conn{
		conn: conn,
		w: connWriter{
			conn:      conn,
			requestCh: make(chan request, requestChanSize),
		},
		r: connReader{
			conn: bufio.NewReaderSize(conn, ioBufferSize),
			h:    make(map[frame.StreamID]responseHandler),
		},
	}
	go c.w.loop()
	go c.r.loop()

	err := c.init()
	return c, err
}

var startupOptions = frame.StartupOptions{"CQL_VERSION": "3.0.0"}

func (c *Conn) init() error {
	res, err := c.Startup(startupOptions)
	if err != nil {
		return err
	}

	switch v := res.(type) {
	case *Ready:
		return nil
	case *Error:
		return fmt.Errorf("init: %s", v.Message)
	default:
		return fmt.Errorf("init: unimplemented response %T, %+v", v, v)
	}
}

// TODO add conn Close, make sure go routines exit

func (c *Conn) Startup(options frame.StartupOptions) (frame.Response, error) {
	return c.sendRequest(&Startup{Options: options}, false, false)
}

func (c *Conn) Query(s Statement, pagingState frame.Bytes) (QueryResult, error) {
	req := newQueryForStatement(s, pagingState)
	res, err := c.sendRequest(req, s.Compression, s.Tracing)
	if err != nil {
		return QueryResult{}, err
	}

	return makeQueryResult(res)
}

func (c *Conn) sendRequest(req frame.Request, compress, tracing bool) (frame.Response, error) {
	h := make(responseHandler)

	streamID, err := c.r.setHandler(h)
	if err != nil {
		return nil, fmt.Errorf("set handler: %w", err)
	}

	r := request{
		Request:         req,
		StreamID:        streamID,
		Compress:        compress,
		Tracing:         tracing,
		ResponseHandler: h,
	}

	c.w.submit(r)

	resp := <-h
	c.r.freeHandler(streamID)

	return resp.Response, resp.Err
}
