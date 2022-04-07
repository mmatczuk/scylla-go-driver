package request

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestAuthResponseWriteTo(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		username string
		password string
		expected []byte
	}{
		{
			name:     "Should encode and decode",
			username: "username",
			password: "password",
			expected: []byte{
				0x00, 0x00, 0x00, 0x12,
				0x00, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65,
				0x00, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64,
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ar := AuthResponse{Username: tc.username, Password: tc.password}
			var out frame.Buffer
			ar.WriteTo(&out)
			if diff := cmp.Diff(out.Bytes(), tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
