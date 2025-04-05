package task

import (
	"github.com/xwb1989/sqlparser"
)

func Partition[T sqlparser.SQLNode](node T) map[sqlparser.TableName]T {
	part := map[sqlparser.TableName]T{}
	for tbl, node := range partition(node) {
		n, _ := node.(T)
		part[tbl] = n
	}
	return part
}

func partition(node sqlparser.SQLNode) map[sqlparser.TableName]sqlparser.SQLNode {
	switch n := node.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
		projects := Partition(n.SelectExprs)
		froms := Partition(n.From)
		wheres := Partition(n.Where)
		groups := Partition(n.GroupBy)
		havings := Partition(n.Having)
		orders := Partition(n.OrderBy)

		parts := map[sqlparser.TableName]sqlparser.SQLNode{}
		for tbl := range froms {
			project := projects[tbl]
			from := froms[tbl]
			where := wheres[tbl]
			group := groups[tbl]
			having := havings[tbl]
			order := orders[tbl]

			if project == nil {
				if len(projects) == 0 {
					project = n.SelectExprs
				} else {
					project = sqlparser.SelectExprs{&sqlparser.StarExpr{TableName: tbl}}
				}
			}

			parts[tbl] = &sqlparser.Select{
				Cache:       n.Cache,
				Comments:    n.Comments,
				Distinct:    n.Distinct,
				Hints:       n.Hints,
				SelectExprs: project,
				From:        from,
				Where:       where,
				GroupBy:     group,
				Having:      having,
				OrderBy:     order,
				Limit:       n.Limit,
				Lock:        n.Lock,
			}
		}
		return parts
	case *sqlparser.Insert:
	case *sqlparser.Update:
	case *sqlparser.Delete:
	case *sqlparser.Set:
	case *sqlparser.DBDDL:
	case *sqlparser.DDL:
	case *sqlparser.Show:
	case *sqlparser.Use:
	case *sqlparser.Begin:
	case *sqlparser.Commit:
	case *sqlparser.Rollback:
	case *sqlparser.OtherRead:
	case *sqlparser.OtherAdmin:
	case *sqlparser.ParenSelect:
	case *sqlparser.Stream:
	case sqlparser.Values:
	case *sqlparser.PartitionSpec:
	case *sqlparser.PartitionDefinition:
	case *sqlparser.TableSpec:
	case *sqlparser.ColumnDefinition:
	case *sqlparser.ColumnType:
	case *sqlparser.IndexDefinition:
	case *sqlparser.IndexInfo:
	case *sqlparser.VindexSpec:
	case *sqlparser.VindexParam:
	case sqlparser.SelectExprs:
		parts := map[sqlparser.TableName]sqlparser.SQLNode{}
		for _, expr := range n {
			for tbl, sub := range Partition(expr) {
				if list, ok := parts[tbl].(sqlparser.SelectExprs); ok {
					parts[tbl] = append(list, sub)
				} else {
					parts[tbl] = sqlparser.SelectExprs{sub}
				}
			}
		}
		return parts
	case *sqlparser.StarExpr:
	case *sqlparser.AliasedExpr:
		parts := map[sqlparser.TableName]sqlparser.SQLNode{}
		for tbl, sub := range Partition(n.Expr) {
			parts[tbl] = &sqlparser.AliasedExpr{Expr: sub, As: n.As}
		}
		return parts
	case *sqlparser.Nextval:
	case sqlparser.Columns:
	case sqlparser.Partitions:
	case sqlparser.TableExprs:
		parts := map[sqlparser.TableName]sqlparser.SQLNode{}
		for _, expr := range n {
			for tbl, sub := range Partition(expr) {
				if list, ok := parts[tbl].(sqlparser.TableExprs); ok {
					parts[tbl] = append(list, sub)
				} else {
					parts[tbl] = sqlparser.TableExprs{sub}
				}
			}
		}
		return parts
	case *sqlparser.AliasedTableExpr:
		tbl := sqlparser.TableName{Name: n.As}
		if tbl.Name.IsEmpty() {
			tbl.Name = sqlparser.NewTableIdent(sqlparser.String(n.Expr))
		}
		return map[sqlparser.TableName]sqlparser.SQLNode{tbl: &sqlparser.AliasedTableExpr{
			Expr:       n.Expr,
			Partitions: n.Partitions,
			Hints:      n.Hints,
		}}
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
	case sqlparser.TableName:
		return map[sqlparser.TableName]sqlparser.SQLNode{n: n}
	case *sqlparser.Subquery:
	case sqlparser.TableNames:
	case *sqlparser.IndexHints:
	case *sqlparser.Where:
	case *sqlparser.AndExpr:
	case *sqlparser.OrExpr:
	case *sqlparser.NotExpr:
	case *sqlparser.ParenExpr:
	case *sqlparser.ComparisonExpr:
	case *sqlparser.RangeCond:
	case *sqlparser.IsExpr:
	case *sqlparser.ExistsExpr:
	case *sqlparser.SQLVal:
	case *sqlparser.NullVal:
	case sqlparser.BoolVal:
	case *sqlparser.ColName:
		return map[sqlparser.TableName]sqlparser.SQLNode{n.Qualifier: &sqlparser.ColName{Name: n.Name}}
	case sqlparser.ValTuple:
	case sqlparser.ListArg:
	case *sqlparser.BinaryExpr:
	case *sqlparser.UnaryExpr:
	case *sqlparser.IntervalExpr:
	case *sqlparser.CollateExpr:
	case *sqlparser.FuncExpr:
	case *sqlparser.CaseExpr:
	case *sqlparser.ValuesFuncExpr:
	case *sqlparser.ConvertExpr:
	case *sqlparser.SubstrExpr:
	case *sqlparser.ConvertUsingExpr:
	case *sqlparser.MatchExpr:
	case *sqlparser.GroupConcatExpr:
	case *sqlparser.Default:
	case sqlparser.Exprs:
	case *sqlparser.ConvertType:
	case *sqlparser.When:
	case *sqlparser.GroupBy:
	case *sqlparser.OrderBy:
	case *sqlparser.Order:
	case *sqlparser.Limit:
	case sqlparser.UpdateExprs:
	case *sqlparser.UpdateExpr:
	case sqlparser.SetExprs:
	case *sqlparser.SetExpr:
	case sqlparser.OnDup:
	case sqlparser.ColIdent:
	case sqlparser.TableIdent:
		return map[sqlparser.TableName]sqlparser.SQLNode{sqlparser.TableName{Name: n}: n}
	}
	return nil
}
