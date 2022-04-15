package scylla

import (
	"fmt"

	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

type Query struct {
	session *Session
	stmt    transport.Statement
	exec    func(*transport.Conn, transport.Statement, frame.Bytes) (transport.QueryResult, error)
	buf     frame.Buffer
}

func (q *Query) Exec() (Result, error) {
	token, tokenAware := q.token()
	info := q.info(token, tokenAware)
	it := q.session.hsp.PlanIter(info)

	var conn *transport.Conn

	if tokenAware {
		conn = it().Conn(token)
	} else {
		conn = it().LeastBusyConn()
	}

	if conn == nil {
		return Result{}, errNoConnection
	}

	res, err := q.exec(conn, q.stmt, nil)
	return Result(res), err
}

func (q *Query) AsyncExec(callback func(Result, error)) {
	// Clone the statement to avoid (bound) values overwrite.
	stmt := q.stmt.Clone()

	go func() {
		var conn *transport.Conn
		if conn == nil {
			callback(Result{}, errNoConnection)
		}

		res, err := q.exec(conn, stmt, nil)
		callback(Result(res), err)
	}()
}

// https://github.com/scylladb/scylla/blob/40adf38915b6d8f5314c621a94d694d172360833/compound_compat.hh#L33-L47
func (q *Query) token() (transport.Token, bool) {
	if q.stmt.PkCnt == 0 {
		return 0, false
	}

	q.buf.Reset()
	if q.stmt.PkCnt == 1 {
		return transport.MurmurToken(q.stmt.Values[q.stmt.PkIndexes[0]].Bytes), true
	}
	for _, idx := range q.stmt.PkIndexes {
		size := q.stmt.Values[idx].N
		q.buf.WriteShort(frame.Short(size))
		q.buf.Write(q.stmt.Values[idx].Bytes)
		q.buf.WriteByte(0)
	}

	return transport.MurmurToken(q.buf.Bytes()), true
}

func (q *Query) info(token transport.Token, tokenAware bool) transport.QueryInfo {
	if tokenAware {
		// TODO: Will the driver support using different keyspaces than default?
		if info, err := q.session.cluster.NewTokenAwareQueryInfo(token, ""); err == nil {
			return info
		}
	}

	return q.session.cluster.NewQueryInfo()
}

func (q *Query) BindInt64(pos int, v int64) *Query {
	p := &q.stmt.Values[pos]
	if p.N == 0 {
		p.N = 8
		p.Bytes = make([]byte, 8)
	}

	p.Bytes[0] = byte(v >> 56)
	p.Bytes[1] = byte(v >> 48)
	p.Bytes[2] = byte(v >> 40)
	p.Bytes[3] = byte(v >> 32)
	p.Bytes[4] = byte(v >> 24)
	p.Bytes[5] = byte(v >> 16)
	p.Bytes[6] = byte(v >> 8)
	p.Bytes[7] = byte(v)

	return q
}

func (q *Query) SetPageSize(v int32) {
	q.stmt.PageSize = v
}

func (q *Query) PageSize() int32 {
	return q.stmt.PageSize
}

type Result transport.QueryResult

func (q *Query) Iter() Iter {
	it := Iter{
		requestCh: make(chan struct{}, 1),
		nextCh:    make(chan transport.QueryResult),
		errCh:     make(chan error, 1),
	}

	worker := iterWorker{
		stmt: q.stmt.Clone(),
		chooseConn: func() *transport.Conn {
			return q.session.leastBusyConn()
		},
		queryFn:   q.exec,
		requestCh: it.requestCh,
		nextCh:    it.nextCh,
		errCh:     it.errCh,
	}

	it.requestCh <- struct{}{}
	go worker.loop()
	return it
}

type Iter struct {
	result transport.QueryResult
	pos    int
	rowCnt int

	requestCh chan struct{}
	nextCh    chan transport.QueryResult
	errCh     chan error
	closed    bool
}

var (
	ErrClosedIter = fmt.Errorf("iter is closed")
	ErrNoMoreRows = fmt.Errorf("no more rows left")
)

func (it *Iter) Next() (frame.Row, error) {
	if it.closed {
		return nil, ErrClosedIter
	}

	if it.pos >= it.rowCnt {
		var ok bool
		it.result, ok = <-it.nextCh
		if !ok {
			it.Close()
			return nil, <-it.errCh
		}

		it.pos = 0
		it.rowCnt = len(it.result.Rows)
		it.requestCh <- struct{}{}
	}

	// We probably got a zero-sized last page, retry to be sure
	if it.rowCnt == 0 {
		return it.Next()
	}

	res := it.result.Rows[it.pos]
	it.pos++
	return res, nil
}

func (it *Iter) Close() {
	if it.closed {
		return
	}

	it.closed = true
	close(it.requestCh)
	_, _ = <-it.nextCh
}

type iterWorker struct {
	stmt        transport.Statement
	chooseConn  func() *transport.Conn
	pagingState []byte
	queryFn     func(*transport.Conn, transport.Statement, frame.Bytes) (transport.QueryResult, error)

	requestCh chan struct{}
	nextCh    chan transport.QueryResult
	errCh     chan error
}

func (w *iterWorker) loop() {
	for {
		_, ok := <-w.requestCh
		if !ok {
			return
		}
		conn := w.chooseConn()
		res, err := w.queryFn(conn, w.stmt, w.pagingState)
		if err != nil {
			close(w.nextCh)
			w.errCh <- err
			return
		}
		w.pagingState = res.PagingState
		w.nextCh <- res

		if !res.HasMorePages {
			close(w.nextCh)
			w.errCh <- ErrNoMoreRows
			return
		}
	}
}
