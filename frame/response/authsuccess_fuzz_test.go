package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

var dummyAS *AuthSuccess

// We want to make sure that parsing does not crush driver even for random data.
// We assign result to global variable to avoid compiler optimization.
func FuzzAuthSuccess(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var buf frame.Buffer
		buf.Write(data)
		out := ParseAuthSuccess(&buf)
		dummyAS = out
	})
}