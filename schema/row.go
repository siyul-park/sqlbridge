package schema

import (
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Row struct {
	Columns  []*sqlparser.ColName
	Values   []sqltypes.Value
	Children []Row
}

func (r *Row) IsEmpty() bool {
	return len(r.Columns) == 0 && len(r.Values) == 0 && len(r.Children) == 0
}
