package response

import (
	"log"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

// Result spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L546
// Below are types of Result with different bodies.

// VoidResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L562
type VoidResult struct{}

func ParseVoidResult(_ *frame.Buffer) *VoidResult {
	return &VoidResult{}
}

// RowsResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L568
type RowsResult struct {
	Metadata    frame.ResultMetadata
	RowsCnt     frame.Int
	RowsContent []frame.Row
}

func ParseRowsResult(b *frame.Buffer) *RowsResult {
	r := RowsResult{
		Metadata: b.ReadResultMetadata(),
		RowsCnt:  b.ReadInt(),
	}

	r.RowsContent = make([]frame.Row, r.RowsCnt)
	for i := range r.RowsContent {
		r.RowsContent[i] = make(frame.Row, r.Metadata.ColumnsCnt)
		for j := 0; j < int(r.Metadata.ColumnsCnt); j++ {
			r.RowsContent[i][j] = frame.CqlValue{
				Value: b.ReadBytes(),
			}

			if r.Metadata.Columns != nil {
				r.RowsContent[i][j].Type = &r.Metadata.Columns[j].Type
			}
		}
	}

	return &r
}

// SetKeyspaceResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L669
type SetKeyspaceResult struct {
	Name string
}

func ParseSetKeyspaceResult(b *frame.Buffer) *SetKeyspaceResult {
	return &SetKeyspaceResult{
		Name: b.ReadString(),
	}
}

// PreparedResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L675
type PreparedResult struct {
	ID             frame.ShortBytes
	Metadata       frame.PreparedMetadata
	ResultMetadata frame.ResultMetadata
}

func ParsePreparedResult(b *frame.Buffer) *PreparedResult {
	return &PreparedResult{
		ID:             b.ReadShortBytes(),
		Metadata:       b.ReadPreparedMetadata(),
		ResultMetadata: b.ReadResultMetadata(),
	}
}

// SchemaChangeResult spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L742
type SchemaChangeResult struct {
	SchemaChange SchemaChange
}

func ParseSchemaChangeResult(b *frame.Buffer) *SchemaChangeResult {
	return &SchemaChangeResult{
		SchemaChange: *ParseSchemaChange(b),
	}
}

type ResultKind = frame.Int

const (
	VoidKind         ResultKind = 1
	RowsKind         ResultKind = 2
	SetKeySpaceKind  ResultKind = 3
	PreparedKind     ResultKind = 4
	SchemaChangeKind ResultKind = 5
)

func ParseResult(b *frame.Buffer) frame.Response {
	resultKind := b.ReadInt()
	switch resultKind {
	case VoidKind:
		return ParseVoidResult(b)
	case RowsKind:
		return ParseRowsResult(b)
	case SetKeySpaceKind:
		return ParseSetKeyspaceResult(b)
	case PreparedKind:
		return ParsePreparedResult(b)
	case SchemaChangeKind:
		return ParseSchemaChangeResult(b)
	default:
		log.Printf("result kind not supported: %d", resultKind)
		return nil
	}
}
