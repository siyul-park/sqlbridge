package task

import (
	"errors"
	"fmt"
)

var ErrUnsupportedType = errors.New("unsupported type")
var ErrUnsupportedValue = errors.New("unsupported value")

func NewErrUnsupportedType(value any) error {
	return fmt.Errorf("%w: %T", ErrUnsupportedType, value)
}

func NewErrUnsupportedValue(value any) error {
	return fmt.Errorf("%w: %v", ErrUnsupportedValue, value)
}
