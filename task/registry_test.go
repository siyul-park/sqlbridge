package task

import (
	"database/sql/driver"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"testing"
)

func TestRegistry_AddBuilder(t *testing.T) {
	registry := NewRegistry()

	b := Build(func(node sqlparser.SQLNode) (Task, error) {
		return nil, driver.ErrSkip
	})

	ok := registry.AddBuilder(b)
	require.True(t, ok)
}

func TestRegistry_RemoveBuilder(t *testing.T) {
	registry := NewRegistry()

	b := Build(func(node sqlparser.SQLNode) (Task, error) {
		return nil, driver.ErrSkip
	})

	ok := registry.AddBuilder(b)
	require.True(t, ok)

	ok = registry.RemoveBuilder(b)
	require.True(t, ok)
}
