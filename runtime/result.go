package runtime

import (
	"sort"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/value"
)

// Result accumulates the rows produced by a compiled query program. The program
// pushes one value per output column, optionally pushes ORDER BY sort keys, and
// commits a row at a time; the driver reads the finished rows after execution.
type Result struct {
	columns    []*sqlparser.ColName
	current    []sqltypes.Value
	currentKey []value.Value
	rows       []catalog.Row
	keys       [][]value.Value
}

// NewResult creates an accumulator for the given output columns.
func NewResult(columns []*sqlparser.ColName) *Result {
	return &Result{columns: columns}
}

// Push appends a single value to the row currently being built.
func (r *Result) Push(v value.Value) error {
	sv, err := toSQL(v)
	if err != nil {
		return err
	}
	r.current = append(r.current, sv)
	return nil
}

// PushKey appends an ORDER BY sort key for the row currently being built.
func (r *Result) PushKey(v value.Value) {
	r.currentKey = append(r.currentKey, v)
}

// Commit finalizes the current projected row and its sort keys.
func (r *Result) Commit() {
	r.rows = append(r.rows, catalog.Row{
		Columns: append([]*sqlparser.ColName(nil), r.columns...),
		Values:  r.current,
	})
	r.keys = append(r.keys, r.currentKey)
	r.current = nil
	r.currentKey = nil
}

// EmitRow copies an entire input row into the result (SELECT *), keeping any
// sort keys pushed for it.
func (r *Result) EmitRow(row catalog.Row) {
	r.rows = append(r.rows, catalog.Row{
		Columns: append([]*sqlparser.ColName(nil), row.Columns...),
		Values:  append([]sqltypes.Value(nil), row.Values...),
	})
	r.keys = append(r.keys, r.currentKey)
	r.currentKey = nil
}

// Sort orders the accumulated rows by their pushed sort keys. desc[i] requests
// descending order for the i-th key.
func (r *Result) Sort(desc []bool) {
	type pair struct {
		row catalog.Row
		key []value.Value
	}
	pairs := make([]pair, len(r.rows))
	for i := range r.rows {
		pairs[i] = pair{row: r.rows[i], key: r.keys[i]}
	}
	sort.SliceStable(pairs, func(a, b int) bool {
		ka, kb := pairs[a].key, pairs[b].key
		for i := 0; i < len(ka) && i < len(kb); i++ {
			c := compareKey(ka[i], kb[i])
			if c == 0 {
				continue
			}
			if i < len(desc) && desc[i] {
				return c > 0
			}
			return c < 0
		}
		return false
	})
	for i := range pairs {
		r.rows[i] = pairs[i].row
		r.keys[i] = pairs[i].key
	}
}

// Distinct removes duplicate rows, preserving first-seen order and any aligned
// sort keys.
func (r *Result) Distinct() {
	seen := make(map[string]struct{}, len(r.rows))
	rows := r.rows[:0:0]
	keys := r.keys[:0:0]
	for i, row := range r.rows {
		h := hashRow(row)
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		rows = append(rows, row)
		if i < len(r.keys) {
			keys = append(keys, r.keys[i])
		}
	}
	r.rows = rows
	r.keys = keys
}

// hashRow builds a canonical key from a row's column values.
func hashRow(row catalog.Row) string {
	var sb strings.Builder
	for _, v := range row.Values {
		sb.WriteString(strconv.Itoa(int(v.Type())))
		sb.WriteByte(':')
		sb.Write(v.Raw())
		sb.WriteByte(';')
	}
	return sb.String()
}

// Limit applies an OFFSET/row-count window to the accumulated rows. A negative
// count means no row-count limit.
func (r *Result) Limit(offset, count int64) {
	if offset < 0 {
		offset = 0
	}
	if offset >= int64(len(r.rows)) {
		r.rows = nil
		return
	}
	r.rows = r.rows[offset:]
	if count >= 0 && count < int64(len(r.rows)) {
		r.rows = r.rows[:count]
	}
}

// appendValues appends a fully-formed output row from SQL values.
func (r *Result) appendValues(columns []*sqlparser.ColName, vals []value.Value) error {
	row := catalog.Row{Columns: append([]*sqlparser.ColName(nil), columns...)}
	for _, v := range vals {
		sv, err := toSQL(v)
		if err != nil {
			return err
		}
		row.Values = append(row.Values, sv)
	}
	r.rows = append(r.rows, row)
	r.keys = append(r.keys, nil)
	return nil
}

// Rows returns the accumulated rows.
func (r *Result) Rows() []catalog.Row {
	return r.rows
}

// toSQL converts a SQL value to its sqltypes form, mapping nil to NULL.
func toSQL(v value.Value) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	return value.ToSQL(v, v.Type())
}

// compareKey orders two sort-key values, placing NULL before any value.
func compareKey(a, b value.Value) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	c, err := value.Compare(a, b)
	if err != nil {
		return 0
	}
	return c
}
