package driver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransaction_Commit(t *testing.T) {
	tx := &transaction{}

	err := tx.Commit()
	require.NoError(t, err)
}

func TestTransaction_Rollback(t *testing.T) {
	tx := &transaction{}

	err := tx.Rollback()
	require.NoError(t, err)
}
