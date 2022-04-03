package request

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
)

var _ frame.Request = (*AuthResponse)(nil)

// AuthResponse spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L311
type AuthResponse struct {
	Token frame.Bytes
}

func (a *AuthResponse) WriteTo(b *frame.Buffer) {
	b.WriteBytes(a.Token)
}

func (*AuthResponse) OpCode() frame.OpCode {
	return frame.OpAuthResponse
}
