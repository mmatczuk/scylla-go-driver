package request

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
)

var _ frame.Request = (*AuthResponse)(nil)

// AuthResponse currently only supports login and password authentication,
// so it stores them instead of token for convenience.
// Spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L311
type AuthResponse struct {
	Username string
	Password string
}

func (a *AuthResponse) WriteTo(b *frame.Buffer) {
	b.WriteLongString("\x00" + a.Username + "\x00" + a.Password)
}

func (*AuthResponse) OpCode() frame.OpCode {
	return frame.OpAuthResponse
}
