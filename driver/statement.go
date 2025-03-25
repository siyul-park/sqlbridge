package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/siyul-park/sqlbridge/vm"
	"github.com/xwb1989/sqlparser"
)

type Statement struct {
	vm    *vm.VM
	query string
}

var _ driver.Stmt = (*Statement)(nil)
var _ driver.StmtExecContext = (*Statement)(nil)
var _ driver.StmtQueryContext = (*Statement)(nil)

func (s *Statement) NumInput() int {
	num := 0
	for i := 0; i < len(s.query); i++ {
		switch s.query[i] {
		case '?', ':':
			num++
		default:
		}
	}
	return num
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), s.named(args))
}

func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), s.named(args))
}

func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	query := s.format(s.anonymized(s.query, args))
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	return s.vm.Exec(ctx, stmt)
}

func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query := s.format(s.anonymized(s.query, args))
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	return s.vm.Query(ctx, stmt)
}

func (s *Statement) Close() error {
	return nil
}

func (s *Statement) format(query string, args []driver.Value) string {
	var builder strings.Builder
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			val := args[0]
			if v, ok := val.(driver.Valuer); ok {
				val, _ = v.Value()
			}

			switch val := val.(type) {
			case nil:
				builder.WriteString("NULL")
			case string:
				builder.WriteString(fmt.Sprintf("'%s'", s.escape(val)))
			case []byte:
				builder.WriteString(fmt.Sprintf("'%s'", s.escape(string(val))))
			case time.Time:
				builder.WriteString(fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")))
			case bool:
				if val {
					builder.WriteString("TRUE")
				} else {
					builder.WriteString("FALSE")
				}
			case float64:
				builder.WriteString(strconv.FormatFloat(val, 'e', 12, 64))
			case float32:
				builder.WriteString(strconv.FormatFloat(float64(val), 'e', 12, 32))
			default:
				builder.WriteString(fmt.Sprintf("%v", val))
			}
			args = args[1:]
		} else {
			builder.WriteByte(query[i])
		}
	}
	return builder.String()
}

func (s *Statement) named(args []driver.Value) []driver.NamedValue {
	value := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		value = append(value, driver.NamedValue{Ordinal: i, Value: arg})
	}
	return value
}

func (s *Statement) anonymized(query string, args []driver.NamedValue) (string, []driver.Value) {
	if len(args) == 0 {
		return query, nil
	}

	named := make(map[string]driver.Value, len(args))
	anonymized := make([]driver.Value, len(args))
	for _, arg := range args {
		if arg.Name != "" {
			named[arg.Name] = arg.Value
		} else {
			anonymized[arg.Ordinal] = arg.Value
		}
	}

	var builder strings.Builder
	var values []driver.Value
	for i := 0; i < len(query); i++ {
		// TODO: ignore \?, '?', ...
		switch query[i] {
		case '?':
			values = append(values, anonymized[0])
			anonymized = anonymized[1:]
			builder.WriteByte('?')
		case ':':
			start := i + 1
			for i+1 < len(query) && ((query[i+1] >= 'a' && query[i+1] <= 'z') ||
				(query[i+1] >= 'A' && query[i+1] <= 'Z') ||
				(query[i+1] >= '0' && query[i+1] <= '9') ||
				query[i+1] == '_') {
				i++
			}
			values = append(values, named[query[start:i+1]])
			builder.WriteByte('?')
		default:
			builder.WriteByte(query[i])
		}
	}
	return builder.String(), values
}

func (s *Statement) escape(value string) string {
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case 0:
			builder.WriteString("\\0")
		case '\n':
			builder.WriteString("\\n")
		case '\r':
			builder.WriteString("\\r")
		case '\\':
			builder.WriteString("\\\\")
		case '\'':
			builder.WriteString("\\'")
		case '"':
			builder.WriteString("\\\"")
		case '\032':
			builder.WriteString("\\Z")
		default:
			builder.WriteByte(value[i])
		}
	}
	return builder.String()
}
