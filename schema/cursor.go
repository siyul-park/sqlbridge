package schema

import (
	"errors"
	"io"
)

type Cursor interface {
	Next() (*Record, error)
	Close() error
}

type InMemoryCursor struct {
	records []*Record
	offset  int
}

var _ Cursor = (*InMemoryCursor)(nil)

func ReadAll(cursor Cursor) ([]*Record, error) {
	var records []*Record
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

func NewInMemoryCursor(records []*Record) *InMemoryCursor {
	if len(records) == 0 {
		records = nil
	}
	return &InMemoryCursor{records: records}
}

func (c *InMemoryCursor) Next() (*Record, error) {
	if c.offset >= len(c.records) {
		return nil, io.EOF
	}
	record := c.records[c.offset]
	c.offset++
	return record.Copy(), nil
}

func (c *InMemoryCursor) Close() error {
	return nil
}
