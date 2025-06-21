package schema

import "github.com/xwb1989/sqlparser"

type Index struct {
	Name    string
	Columns []*sqlparser.ColName
}
