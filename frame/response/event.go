package response

import (
	"fmt"

	"scylla-go-driver/frame"
)

// Event spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L754
// Below are types of events with different bodies.

// TopologyChange spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L760
type TopologyChange struct {
	Change  frame.TopologyChangeType
	Address frame.Inet
}

func ParseTopologyChange(b *frame.Buffer) TopologyChange {
	return TopologyChange{
		Change:  b.ReadTopologyChangeType(),
		Address: b.ReadInet(),
	}
}

// StatusChange spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L766
type StatusChange struct {
	Status  frame.StatusChangeType
	Address frame.Inet
}

func ParseStatusChange(b *frame.Buffer) StatusChange {
	return StatusChange{
		Status:  b.ReadStatusChangeType(),
		Address: b.ReadInet(),
	}
}

// SchemaChange spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L771
type SchemaChange struct {
	Change    frame.SchemaChangeType
	Target    frame.SchemaChangeTarget
	Keyspace  string
	Object    string
	Arguments frame.StringList
}

func ParseSchemaChange(b *frame.Buffer) SchemaChange {
	c := b.ReadSchemaChangeType()
	t := b.ReadSchemaChangeTarget()
	switch t {
	case frame.Keyspace:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: b.ReadString(),
		}
	case frame.Table, frame.UserType:
		return SchemaChange{
			Change:   c,
			Target:   t,
			Keyspace: b.ReadString(),
			Object:   b.ReadString(),
		}
	case frame.Function, frame.Aggregate:
		return SchemaChange{
			Change:    c,
			Target:    t,
			Keyspace:  b.ReadString(),
			Object:    b.ReadString(),
			Arguments: b.ReadStringList(),
		}
	default:
		b.RecordError(fmt.Errorf("invalid SchemaChangeTarget: %s", t))
		return SchemaChange{}
	}
}
