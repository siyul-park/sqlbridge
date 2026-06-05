package runtime

import (
	"strconv"
	"strings"

	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/function"
	"github.com/siyul-park/sqlbridge/value"
)

// Slot describes one output column of an aggregated query: either a group-key
// passthrough or an aggregate function applied to a per-row argument.
type Slot struct {
	Aggregate bool
	Name      string // aggregate function name when Aggregate is true
}

// GrouperSpec is the compile-time configuration of a GROUP BY query. The bucket
// state itself is per execution and created by New.
type GrouperSpec struct {
	Columns    []*sqlparser.ColName
	Slots      []Slot
	GroupCount int // number of leading group-identity values pushed per row
	Dispatcher *function.Dispatcher
}

// New creates a fresh grouper for one execution.
func (s *GrouperSpec) New() *Grouper {
	return &Grouper{spec: s, buckets: map[string]*bucket{}}
}

// Grouper accumulates rows into groups and finalizes aggregates.
type Grouper struct {
	spec    *GrouperSpec
	buckets map[string]*bucket
	order   []string
}

type bucket struct {
	slotVals []value.Value   // first-seen value for key slots
	seen     []bool          // whether a key slot has been captured
	aggArgs  [][]value.Value // collected arguments per aggregate slot
}

// Row records one input row. params holds the group-identity values followed by
// one value per output slot, in slot order.
func (g *Grouper) Row(params []value.Value) {
	identity := params[:g.spec.GroupCount]
	values := params[g.spec.GroupCount:]

	key := hashValues(identity)
	b, ok := g.buckets[key]
	if !ok {
		b = &bucket{
			slotVals: make([]value.Value, len(g.spec.Slots)),
			seen:     make([]bool, len(g.spec.Slots)),
			aggArgs:  make([][]value.Value, len(g.spec.Slots)),
		}
		g.buckets[key] = b
		g.order = append(g.order, key)
	}

	for i, slot := range g.spec.Slots {
		if slot.Aggregate {
			b.aggArgs[i] = append(b.aggArgs[i], values[i])
		} else if !b.seen[i] {
			b.slotVals[i] = values[i]
			b.seen[i] = true
		}
	}
}

// groupValues computes the per-group output value rows. An ungrouped aggregate
// over zero rows still yields a single row (the empty group).
func (g *Grouper) groupValues() ([][]value.Value, error) {
	if len(g.order) == 0 && g.spec.GroupCount == 0 {
		g.buckets[""] = &bucket{
			slotVals: make([]value.Value, len(g.spec.Slots)),
			seen:     make([]bool, len(g.spec.Slots)),
			aggArgs:  make([][]value.Value, len(g.spec.Slots)),
		}
		g.order = append(g.order, "")
	}

	out := make([][]value.Value, 0, len(g.order))
	for _, key := range g.order {
		b := g.buckets[key]
		row := make([]value.Value, len(g.spec.Slots))
		for i, slot := range g.spec.Slots {
			if !slot.Aggregate {
				row[i] = b.slotVals[i]
				continue
			}
			v, err := g.spec.Dispatcher.Dispatch(slot.Name, b.aggArgs[i])
			if err != nil {
				return nil, err
			}
			row[i] = v
		}
		out = append(out, row)
	}
	return out, nil
}

// Finalize materializes one row per group into res.
func (g *Grouper) Finalize(res *Result) error {
	rows, err := g.groupValues()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := res.appendValues(g.spec.Columns, row); err != nil {
			return err
		}
	}
	return nil
}

// Cursor exposes the group rows as a scannable cursor, used to apply a HAVING
// filter over the aggregated rows in a second pass.
func (g *Grouper) Cursor() (catalog.Cursor, error) {
	groups, err := g.groupValues()
	if err != nil {
		return nil, err
	}
	rows := make([]catalog.Row, len(groups))
	for i, vals := range groups {
		row := catalog.Row{Columns: g.spec.Columns}
		for _, v := range vals {
			sv, err := toSQL(v)
			if err != nil {
				return nil, err
			}
			row.Values = append(row.Values, sv)
		}
		rows[i] = row
	}
	return catalog.NewInMemoryCursor(rows), nil
}

// hashValues builds a canonical identity key from a tuple of values.
func hashValues(vals []value.Value) string {
	var sb strings.Builder
	for _, v := range vals {
		if v == nil {
			sb.WriteString("\x00null;")
			continue
		}
		sb.WriteString(strconv.Itoa(int(v.Type())))
		sb.WriteByte(':')
		s, err := value.ToString(v)
		if err != nil {
			s = "?"
		}
		sb.WriteString(s)
		sb.WriteByte(';')
	}
	return sb.String()
}

// GroupInitFunc creates a fresh grouper for the execution from spec. Arity 0.
func GroupInitFunc(spec *GrouperSpec) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		sess.grouper = spec.New()
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// GroupRowFunc feeds one row's identity and slot values into the grouper. Its
// arity is the number of values pushed per row. Returns NULL.
func GroupRowFunc(arity int) *interp.HostFunction {
	params := make([]types.Type, arity)
	for i := range params {
		params[i] = types.TypeRef
	}
	typ := &types.FunctionType{Params: params, Returns: []types.Type{types.TypeRef}}
	return interp.NewHostFunction(typ, func(vm *interp.Interpreter, boxed []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		args := make([]value.Value, len(boxed))
		for i, b := range boxed {
			if args[i], err = Unbox(vm, b); err != nil {
				return nil, err
			}
		}
		sess.grouper.Row(args)
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// GroupCursorFunc replaces the session cursor with the aggregated group rows so
// a HAVING filter can be applied in a second scan. Arity 0. Returns NULL.
func GroupCursorFunc() *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		cur, err := sess.grouper.Cursor()
		if err != nil {
			return nil, err
		}
		sess.cursor = cur
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// GroupFinalizeFunc materializes grouped rows into the result. Arity 0.
func GroupFinalizeFunc() *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		if err := sess.grouper.Finalize(sess.result); err != nil {
			return nil, err
		}
		return []types.Boxed{types.BoxedNull}, nil
	})
}
