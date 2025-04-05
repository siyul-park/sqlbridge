package task

import (
	"errors"
	"fmt"
)

var ErrUnsupportedType = errors.New("unsupported type")
var ErrUnsupportedValue = errors.New("unsupported value")
var ErrNotFound = errors.New("not found")

func NewErrUnsupportedType(value any) error {
	return fmt.Errorf("%w: %T", ErrUnsupportedType, value)
}

func NewErrUnsupportedValue(value any) error {
	return fmt.Errorf("%w: %v", ErrUnsupportedValue, value)
}

func NewErrNotFound(key any) error {
	return fmt.Errorf("%w: %v", ErrNotFound, key)
}
