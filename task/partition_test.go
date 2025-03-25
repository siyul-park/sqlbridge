package task

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestPartition(t *testing.T) {
	var tests []struct {
		node   sqlparser.SQLNode
		expect map[sqlparser.TableName]sqlparser.SQLNode
	}

	for _, test := range tests {
		buf := sqlparser.NewTrackedBuffer(nil)
		test.node.Format(buf)
		t.Run(buf.String(), func(t *testing.T) {
			part := Partition(test.node)
			require.Equal(t, test.expect, part)
		})
	}
}
