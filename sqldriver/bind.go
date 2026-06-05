package sqldriver

import (
	"database/sql/driver"
	"strconv"

	"github.com/siyul-park/sqlbridge/value"
)

// namedBinds converts database/sql named/positional arguments into the bind map
// keyed by the parser's placeholder names (":v1", ":name").
func namedBinds(args []driver.NamedValue) map[string]value.Value {
	binds := make(map[string]value.Value, len(args))
	for _, a := range args {
		name := a.Name
		if name == "" {
			name = "v" + strconv.Itoa(a.Ordinal)
		}
		binds[":"+name] = bindValue(a.Value)
	}
	return binds
}

// valueBinds converts positional driver values into the bind map.
func valueBinds(args []driver.Value) map[string]value.Value {
	binds := make(map[string]value.Value, len(args))
	for i, a := range args {
		binds[":v"+strconv.Itoa(i+1)] = bindValue(a)
	}
	return binds
}

func bindValue(v driver.Value) value.Value {
	if v == nil {
		return nil
	}
	return value.NewValue(v)
}
