package task

import (
	"github.com/xwb1989/sqlparser"
)

func Partition[T sqlparser.SQLNode](node T) map[sqlparser.TableIdent]T {
	part := map[sqlparser.TableIdent]T{}
	for tbl, node := range partition(node) {
		n, _ := node.(T)
		part[tbl] = n
	}
	return part
}

func partition(node sqlparser.SQLNode) map[sqlparser.TableIdent]sqlparser.SQLNode {
	switch node := node.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
		columns := Partition(node.SelectExprs)
		froms := Partition(node.From)
		wheres := Partition(node.Where)
		groups := Partition(node.GroupBy)
		havings := Partition(node.Having)
		orders := Partition(node.OrderBy)

		parts := map[sqlparser.TableIdent]sqlparser.SQLNode{}
		for tbl := range froms {
			column := columns[tbl]
			from := froms[tbl]
			where := wheres[tbl]
			group := groups[tbl]
			having := havings[tbl]
			order := orders[tbl]

			if column == nil {
				column = sqlparser.SelectExprs{&sqlparser.StarExpr{}}
			}

			parts[tbl] = &sqlparser.Select{
				Cache:       node.Cache,
				Comments:    node.Comments,
				Distinct:    node.Distinct,
				Hints:       node.Hints,
				SelectExprs: column,
				From:        from,
				Where:       where,
				GroupBy:     group,
				Having:      having,
				OrderBy:     order,
				Limit:       node.Limit,
				Lock:        node.Lock,
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
		parts := map[sqlparser.TableIdent]sqlparser.SQLNode{}
		for _, expr := range node {
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
	case *sqlparser.Nextval:
	case sqlparser.Columns:
	case sqlparser.Partitions:
	case sqlparser.TableExprs:
		parts := map[sqlparser.TableIdent]sqlparser.SQLNode{}
		for _, expr := range node {
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
		return map[sqlparser.TableIdent]sqlparser.SQLNode{node.As: node}
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
	case sqlparser.TableName:
		return map[sqlparser.TableIdent]sqlparser.SQLNode{node.Name: node}
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
		return map[sqlparser.TableIdent]sqlparser.SQLNode{node.Qualifier.Name: node}
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
		return map[sqlparser.TableIdent]sqlparser.SQLNode{node: node}
	}
	return nil
}
