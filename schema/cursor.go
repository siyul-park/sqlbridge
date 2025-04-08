package schema

import (
	"errors"
	"io"
	"sync"
)

type Cursor interface {
	Next() (Record, error)
	Close() error
}

type InMemoryCursor struct {
	records []Record
	offset  int
}

var _ Cursor = (*InMemoryCursor)(nil)

func ReadAll(cursor Cursor) ([]Record, error) {
	var records []Record
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

func NewInMemoryCursor(records []Record) *InMemoryCursor {
	if len(records) == 0 {
		records = nil
	}
	return &InMemoryCursor{records: records}
}

func (c *InMemoryCursor) Next() (Record, error) {
	if c.offset >= len(c.records) {
		return Record{}, io.EOF
	}
	record := c.records[c.offset]
	c.offset++
	return record, nil
}

func (c *InMemoryCursor) Close() error {
	c.offset = len(c.records)
	return nil
}

type MappedCursor struct {
	cursor    Cursor
	transform func(Record) (Record, error)
	close     sync.Once
}

var _ Cursor = (*MappedCursor)(nil)

func NewMappedCursor(cursor Cursor, transform func(Record) (Record, error)) *MappedCursor {
	return &MappedCursor{cursor: cursor, transform: transform}
}

func (c *MappedCursor) Next() (Record, error) {
	for {
		record, err := c.cursor.Next()
		if err != nil {
			return Record{}, err
		}
		record, err = c.transform(record)
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = c.Close()
			}
			return Record{}, err
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
