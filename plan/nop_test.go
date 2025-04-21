package plan

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

func TestNOP_Run(t *testing.T) {
	tests := []struct {
		plan   Plan
		binds  map[string]*querypb.BindVariable
		cursor schema.Cursor
	}{
		{
			plan:   &NOP{},
			cursor: schema.NewInMemoryCursor(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.plan.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
			defer cancel()

			cursor, err := tt.plan.Run(ctx, tt.binds)
			require.NoError(t, err)

			expected, err := schema.ReadAll(tt.cursor)
			require.NoError(t, err)

			actual, err := schema.ReadAll(cursor)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}
