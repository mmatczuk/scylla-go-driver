package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/mmatczuk/scylla-go-driver/frame"
	"github.com/pierrec/lz4/v4"
)

type comprData struct {
	name             string
	uncompressed     []byte
	compressedSnappy []byte
	compressedLz4    []byte
}

func getRandomBytes(n int) []byte {
	out := make([]byte, n)
	for i := 9; i < n; i++ {
		out[i] = byte(rand.Intn(255))
	}
	return out
}

func TestCompression(t *testing.T) {
	t.Parallel()
	testCases := []comprData{
		{
			name:         "utf-8",
			uncompressed: append(make([]byte, 9), []byte("Hello World! こんにちは世界! ¡Hola, Mundo!")...),
		},
		{
			name:         "64kb",
			uncompressed: getRandomBytes(1<<16 + 9),
		},
		{
			name:         "1mb",
			uncompressed: getRandomBytes(1<<20 + 9),
		},
		{
			name:         "8mb",
			uncompressed: getRandomBytes(1<<23 + 9),
		},
		{
			name:         "64mb",
			uncompressed: getRandomBytes(1<<26 + 9),
		},
		{
			name:         "256mb",
			uncompressed: getRandomBytes(1<<28 + 9),
		},
	}
	for i, c := range testCases {
		testCases[i].compressedSnappy = s2.Encode(nil, c.uncompressed[9:])
		testCases[i].compressedLz4 = func() []byte {
			in := make([]byte, lz4.CompressBlockBound(len(c.uncompressed)+4))
			n, err := lz4.CompressBlock(c.uncompressed[9:], in[4:], nil)
			binary.BigEndian.PutUint32(in, uint32(len(c.uncompressed))-9)
			if err != nil {
				t.Fatal("error in making test case", err)
			}
			return in[:n+4]
		}()
	}

	ces := getCompression(t, true, frame.Snappy)
	cds := getCompression(t, false, frame.Snappy)
	cel := getCompression(t, true, frame.Lz4)
	cdl := getCompression(t, false, frame.Lz4)

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Just created compr structs
			testSnappy(t, getCompression(t, true, frame.Snappy), true, &tc)
			testSnappy(t, getCompression(t, false, frame.Snappy), false, &tc)
			testLz4(t, getCompression(t, true, frame.Lz4), true, &tc)
			testLz4(t, getCompression(t, false, frame.Lz4), false, &tc)

			// Long-running compr structs
			testSnappy(t, ces, true, &tc)
			testSnappy(t, cds, false, &tc)
			testLz4(t, cel, true, &tc)
			testLz4(t, cdl, false, &tc)
		})
	}
}

func getCompression(t *testing.T, compress bool, variant frame.Compression) *compr {
	c, err := newCompr(compress, variant, comprBufferSize)
	if err != nil {
		t.Fatal("error in making compr struct", compress, frame.Lz4)
	}
	return c
}

func testSnappy(t *testing.T, c *compr, compress bool, data *comprData) {
	if compress {
		buf := *bytes.NewBuffer(data.uncompressed)
		var in bytes.Buffer
		testCompress(t, c, &in, &buf, data.compressedSnappy)
	} else {
		lr := io.LimitedReader{
			R: bytes.NewBuffer(data.compressedSnappy),
		}
		lr.N = int64(len(data.compressedSnappy))
		var in bytes.Buffer
		testDecompress(t, c, &in, &lr, data.uncompressed)
	}
}

func testLz4(t *testing.T, c *compr, compress bool, data *comprData) {
	if compress {
		buf := *bytes.NewBuffer(data.uncompressed)
		var in bytes.Buffer
		testCompress(t, c, &in, &buf, data.compressedLz4)
	} else {
		lr := io.LimitedReader{
			R: bytes.NewBuffer(data.compressedLz4),
		}
		lr.N = int64(len(data.compressedLz4))
		var in bytes.Buffer
		testDecompress(t, c, &in, &lr, data.uncompressed)
	}
}

func testCompress(t *testing.T, c *compr, dst, src *bytes.Buffer, target []byte) {
	n, err := c.compress(context.Background(), dst, src)
	if err != nil {
		t.Fatal("error in compression", err)
	}
	if int(n)-9 != len(target) {
		t.Fatal("compressed lengths don't match, expected:", int(n)-9, "got:", len(target))
	}
	if !bytes.Equal(dst.Bytes()[9:], target) {
		t.Fatal("wrong data compression")
	}
}

func testDecompress(t *testing.T, c *compr, dst *bytes.Buffer, src *io.LimitedReader, target []byte) {
	n, err := c.decompress(dst, src)
	if err != nil {
		t.Fatal("error in decompression", err)
	}
	if int(n) != len(target)-9 {
		t.Fatal("decompressed lengths don't match, expected:", int(n)-9, "got:", len(target)-9)
	}
	if !bytes.Equal(dst.Bytes(), target[9:]) {
		t.Fatal("wrong data decompression")
	}
}
