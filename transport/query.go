package transport

import (
	"github.com/mmatczuk/scylla-go-driver/frame"
	. "github.com/mmatczuk/scylla-go-driver/frame/request"
	. "github.com/mmatczuk/scylla-go-driver/frame/response"
)

type Statement struct {
	ID                frame.Bytes
	Content           string
	Values            []frame.Value
	PageSize          frame.Int
	Consistency       frame.Consistency
	SerialConsistency frame.Consistency
	Tracing           bool
	Compression       bool
}

func (s Statement) Clone() Statement {
	v := s

	v.Values = make([]frame.Value, len(s.Values))
	copy(v.Values, s.Values)

	return v
}

func makeQuery(s Statement, pagingState frame.Bytes) Query {
	return Query{
		Query:       s.Content,
		Consistency: s.Consistency,
		Options: frame.QueryOptions{
			Values:            s.Values,
			SerialConsistency: s.SerialConsistency,
			PagingState:       pagingState,
			PageSize:          s.PageSize,
		},
	}
}

func makeExecute(s Statement, pagingState frame.Bytes) Execute {
	return Execute{
		ID:          s.ID,
		Consistency: s.Consistency,
		Options: frame.QueryOptions{
			Values:            s.Values,
			SerialConsistency: s.SerialConsistency,
			PagingState:       pagingState,
			PageSize:          s.PageSize,
		},
	}
}

func makeStatement(cql string) Statement {
	return Statement{
		Content:     cql,
		Consistency: frame.ONE,
	}
}

type QueryResult struct {
	Rows        []frame.Row
	Warnings    []string
	TracingID   frame.UUID
	PagingState frame.Bytes
	ColSpec     []frame.ColumnSpec
}

func makeQueryResult(res frame.Response) (QueryResult, error) {
	switch v := res.(type) {
	case *RowsResult:
		return QueryResult{
			Rows:        v.RowsContent,
			PagingState: v.Metadata.PagingState,
			ColSpec:     v.Metadata.Columns,
		}, nil
	case *VoidResult, *SchemaChangeResult, *SetKeyspaceResult:
		return QueryResult{}, nil
	default:
		return QueryResult{}, responseAsError(res)
	}
}
