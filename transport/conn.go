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
	"unicode"

	"scylla-go-driver/frame"
	. "scylla-go-driver/frame/request"
	. "scylla-go-driver/frame/response"

	"go.uber.org/atomic"
)

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

var _connCloseRequest = request{}

type connMetrics struct {
	InFlight atomic.Uint32
	InQueue  atomic.Uint32
}

type connWriter struct {
	conn       *bufio.Writer
	buf        frame.Buffer
	requestCh  chan request
	metrics    *connMetrics
	connString func() string
	connClose  func()
}

func (c *connWriter) submit(r request) {
	c.metrics.InQueue.Inc()
	c.requestCh <- r
}

func (c *connWriter) loop() {
	runtime.LockOSThread()

	for {
		size := len(c.requestCh)
		if size > maxCoalescedRequests {
			size = maxCoalescedRequests
		} else if size == 0 {
			size = 1
		}

		for i := 0; i < size; i++ {
			r := <-c.requestCh
			c.metrics.InQueue.Dec()
			if r == _connCloseRequest {
				return
			}
			if err := c.send(r); err != nil {
				log.Printf("%s fatal send error, closing connection due to %s", c.connString(), err)
				r.ResponseHandler <- response{Err: fmt.Errorf("%s send: %w", c.connString(), err)}
				c.connClose()
				return
			}
			c.metrics.InFlight.Inc()
		}
		if err := c.conn.Flush(); err != nil {
			log.Printf("%s fatal flush error, closing connection due to %s", c.connString(), err)
			c.connClose()
			return
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
	conn        *bufio.Reader
	buf         frame.Buffer
	bufw        io.Writer
	metrics     *connMetrics
	handleEvent func(r response)
	connString  func() string
	connClose   func()

	h      map[frame.StreamID]responseHandler
	s      streamIDAllocator
	closed bool
	mu     sync.Mutex // mu guards h, s and closed
}

func (c *connReader) setHandler(h responseHandler) (frame.StreamID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, fmt.Errorf("%s closed", c.connString())
	}

	streamID, err := c.s.Alloc()
	if err != nil {
		return 0, fmt.Errorf("%s stream ID alloc: %w", c.connString(), err)
	}

	c.h[streamID] = h
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
		if resp.StreamID == eventStreamID {
			if c.handleEvent != nil {
				c.handleEvent(resp)
			} else {
				log.Printf("%s received event: %#+v", c.connString(), resp)
			}
			continue
		}

		if resp.Err != nil {
			log.Printf("%s fatal receive error, closing connection due to %s", c.connString(), resp.Err)
			c.connClose()
			c.drainHandlers()
			return
		}

		c.metrics.InFlight.Dec()

		if h := c.handler(resp.StreamID); h != nil {
			h <- resp
		} else {
			log.Printf("%s received unknown stream ID %d, closing connection", c.connString(), resp.StreamID)
			c.connClose()
			c.drainHandlers()
			return
		}
	}
}

func (c *connReader) drainHandlers() {
	c.mu.Lock()
	c.closed = true
	for _, h := range c.h {
		h <- response{Err: fmt.Errorf("%s closed", c.connString())}
	}
	c.mu.Unlock()
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
	if r.Response == nil {
		r.Err = fmt.Errorf("response type not supported")
		return r
	}
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
	case frame.OpEvent:
		return ParseEvent(&c.buf)
	default:
		log.Fatalf("not supported %d", op)
		return nil
	}
}

type Conn struct {
	conn      net.Conn
	shard     uint16
	w         connWriter
	r         connReader
	metrics   *connMetrics
	closeOnce sync.Once
	onClose   func(conn *Conn)
}

type ConnConfig struct {
	Keyspace           string
	TCPNoDelay         bool
	Timeout            time.Duration
	DefaultConsistency frame.Consistency
}

const (
	requestChanSize      = maxStreamID / 2
	maxCoalescedRequests = 100
	ioBufferSize         = 8192
)

// OpenShardConn opens connection mapped to a specific shard on Scylla node.
func OpenShardConn(addr string, si ShardInfo, cfg ConnConfig) (*Conn, error) { // nolint:unused // This will be used.
	it := ShardPortIterator(si)
	maxTries := (maxPort-minPort+1)/int(si.NrShards) + 1
	for i := 0; i < maxTries; i++ {
		conn, err := OpenLocalPortConn(addr, it(), cfg)
		if err != nil {
			log.Printf("%s dial error: %s (try %d/%d)", addr, err, i, maxTries)
			if conn != nil {
				conn.Close()
			}
			continue
		}
		return conn, nil
	}

	return nil, fmt.Errorf("failed to open connection on shard %d: all local ports are busy", si.Shard)
}

// OpenLocalPortConn opens connection on a given local port.
//
// If error and connection are returned the connection is not valid and must be closed by the caller.
func OpenLocalPortConn(addr string, localPort uint16, cfg ConnConfig) (*Conn, error) {
	localAddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(int(localPort)))
	if err != nil {
		return nil, fmt.Errorf("resolve local TCP address: %w", err)
	}

	return OpenConn(addr, localAddr, cfg)
}

// OpenConn opens connection with specific local address.
// In case lAddr is nil, random local address is used.
//
// If error and connection are returned the connection is not valid and must be closed by the caller.
func OpenConn(addr string, localAddr *net.TCPAddr, cfg ConnConfig) (*Conn, error) {
	d := net.Dialer{
		Timeout:   cfg.Timeout,
		LocalAddr: localAddr,
	}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial TCP address %s: %w", addr, err)
	}

	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetNoDelay(cfg.TCPNoDelay); err != nil {
		return nil, fmt.Errorf("set TCP no delay option: %w", err)
	}

	return WrapConn(tcpConn, cfg)
}

// WrapConn transforms tcp connection to a working Scylla connection.
// If error and connection are returned the connection is not valid and must be closed by the caller.
func WrapConn(conn net.Conn, cfg ConnConfig) (*Conn, error) {
	m := new(connMetrics)

	c := new(Conn)
	*c = Conn{
		conn: conn,
		w: connWriter{
			conn:       bufio.NewWriterSize(conn, ioBufferSize),
			requestCh:  make(chan request, requestChanSize),
			metrics:    m,
			connString: c.String,
			connClose:  c.Close,
		},
		r: connReader{
			conn:       bufio.NewReaderSize(conn, ioBufferSize),
			metrics:    m,
			h:          make(map[frame.StreamID]responseHandler),
			connString: c.String,
			connClose:  c.Close,
		},
		metrics: m,
	}

	go c.w.loop()
	go c.r.loop()

	if err := c.init(); err != nil {
		return c, err
	}

	if cfg.Keyspace != "" {
		if err := c.UseKeyspace(cfg.Keyspace); err != nil {
			return c, fmt.Errorf("use keyspace %w", err)
		}
	}

	log.Printf("%s connected", c)

	return c, nil
}

func (cfg *ConnConfig) validate() error {
	if cfg.Keyspace != "" {
		if err := validateKeyspace(cfg.Keyspace); err != nil {
			return err
		}
	}
	if cfg.DefaultConsistency < frame.ANY || cfg.DefaultConsistency > frame.LOCALONE {
		return fmt.Errorf("unknown consistency: %v", cfg.DefaultConsistency)
	}
	return nil
}

func validateKeyspace(keyspace string) error {
	if keyspace == "" || len(keyspace) > 48 {
		return fmt.Errorf("keyspace: invalid length")
	}

	for _, c := range keyspace {
		if !(unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_') {
			return fmt.Errorf("keyspace: illegal characters present")
		}
	}
	return nil
}

var startupOptions = frame.StartupOptions{"CQL_VERSION": "3.0.0"}

func (c *Conn) init() error {
	if s, err := c.Supported(); err != nil {
		return fmt.Errorf("supported: %w", err)
	} else {
		c.shard = s.ScyllaSupported().Shard
	}
	if err := c.Startup(startupOptions); err != nil {
		return fmt.Errorf("startup: %w", err)
	}
	return nil
}

func (c *Conn) Supported() (*Supported, error) {
	res, err := c.sendRequest(&Options{}, false, false)
	if err != nil {
		return nil, err
	}
	if v, ok := res.(*Supported); ok {
		return v, nil
	}
	return nil, responseAsError(res)
}

func (c *Conn) Startup(options frame.StartupOptions) error {
	res, err := c.sendRequest(&Startup{Options: options}, false, false)
	if err != nil {
		return err
	}
	if _, ok := res.(*Ready); ok {
		return nil
	}
	return responseAsError(res)
}

func (c *Conn) UseKeyspace(ks string) error {
	_, err := c.Query(newStatementFromCQL(fmt.Sprintf("USE %q", ks)), nil)
	return err
}

func (c *Conn) Query(s Statement, pagingState frame.Bytes) (QueryResult, error) {
	req := makeQueryForStatement(s, pagingState)
	res, err := c.sendRequest(&req, s.compression, s.tracing)
	if err != nil {
		return QueryResult{}, err
	}

	return makeQueryResult(res)
}

func (c *Conn) QueryAsync(s Statement, pagingState frame.Bytes, fn func(QueryResult, error)) {
	go func() {
		fn(c.Query(s, pagingState))
	}()
}

func (c *Conn) RegisterEventHandler(h func(r response), e ...frame.EventType) error {
	c.r.handleEvent = h
	res, err := c.sendRequest(&Register{EventTypes: e}, false, false)
	if err != nil {
		return err
	}
	if _, ok := res.(*Ready); ok {
		return nil
	}
	return responseAsError(res)
}

func (c *Conn) sendRequest(req frame.Request, compress, tracing bool) (frame.Response, error) {
	// Each handler may encounter 2 responses, one from connWriter.loop()
	// and one from drainHandlers().
	h := make(responseHandler, 2)

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

	// requestCh might be full after terminating writeLoop so some goroutines could hang here forever.
	// this could be fixed by changing requestChanSize to be able to hold all possible streamIDs,
	// adding a grace period before terminating writeLoop or counting active streams.
	c.w.submit(r)

	resp := <-h
	c.r.freeHandler(streamID)

	return resp.Response, resp.Err
}

func (c *Conn) Waiting() int {
	return int(c.metrics.InQueue.Load() + c.metrics.InFlight.Load())
}

func (c *Conn) setOnClose(f func(conn *Conn)) {
	c.onClose = f
}

func (c *Conn) Shard() int {
	return int(c.shard)
}

// Close closes connection and terminates reader and writer go routines.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		if err := c.conn.Close(); err != nil {
			log.Printf("%s failed to close: %s", c, err)
		} else {
			log.Printf("%s closed", c)
		}
		c.w.requestCh <- _connCloseRequest
		if c.onClose != nil {
			c.onClose(c)
		}
	})
}

func (c *Conn) String() string {
	return fmt.Sprintf("[addr=%s shard=%d]", c.conn.RemoteAddr(), c.shard)
}
