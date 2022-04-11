package scylla

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/mmatczuk/scylla-go-driver/transport"
)

type Query struct {
	session *Session
	stmt    transport.Statement
	exec    func(*transport.Conn, transport.Statement, frame.Bytes) (transport.QueryResult, error)
}

func (q *Query) Exec() (Result, error) {
	conn := q.session.leastBusyConn()
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
		conn := q.session.leastBusyConn()
		if conn == nil {
			callback(Result{}, errNoConnection)
		}

		res, err := q.exec(conn, stmt, nil)
		callback(Result(res), err)
	}()
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

type Result transport.QueryResult
