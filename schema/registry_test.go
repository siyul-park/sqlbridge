package schema

import (
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"
)

func TestRegistry_AddSchema(t *testing.T) {
	registry := NewRegistry()

	name := faker.Word()
	sc := New(nil)

	ok := registry.AddSchema(name, sc)
	require.True(t, ok)
}

func TestRegistry_RemoveSchema(t *testing.T) {
	registry := NewRegistry()

	name := faker.Word()
	sc := New(nil)

	ok := registry.AddSchema(name, sc)
	require.True(t, ok)

	ok = registry.RemoveSchema(name)
	require.True(t, ok)
}
