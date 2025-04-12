package schema

import (
	"errors"
	"io"
	"sync"
)

type Cursor interface {
	Next() (Row, error)
	Close() error
}

type InMemoryCursor struct {
	rows   []Row
	offset int
}

var _ Cursor = (*InMemoryCursor)(nil)

func ReadAll(cursor Cursor) ([]Row, error) {
	var records []Row
	for {
		record, err := cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			_ = cursor.Close()
			return nil, err
		}
		records = append(records, record)
	}
	_ = cursor.Close()
	return records, nil
}

func NewInMemoryCursor(records []Row) *InMemoryCursor {
	if len(records) == 0 {
		records = nil
	}
	return &InMemoryCursor{rows: records}
}

func (c *InMemoryCursor) Next() (Row, error) {
	if c.offset >= len(c.rows) {
		return Row{}, io.EOF
	}
	record := c.rows[c.offset]
	c.offset++
	return record, nil
}

func (c *InMemoryCursor) Close() error {
	c.offset = len(c.rows)
	return nil
}

type MappedCursor struct {
	cursor    Cursor
	transform func(Row) (Row, error)
	close     sync.Once
}

var _ Cursor = (*MappedCursor)(nil)

func NewMappedCursor(cursor Cursor, transform func(Row) (Row, error)) *MappedCursor {
	return &MappedCursor{cursor: cursor, transform: transform}
}

func (c *MappedCursor) Next() (Row, error) {
	for {
		record, err := c.cursor.Next()
		if err != nil {
			return Row{}, err
		}
		record, err = c.transform(record)
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = c.Close()
			}
			return Row{}, err
		}
		if !record.IsEmpty() {
			return record, nil
		}
	}
}

func (c *MappedCursor) Close() error {
	var err error
	c.close.Do(func() { err = c.cursor.Close() })
	return err
}
