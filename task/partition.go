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
	switch node.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
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
	case *sqlparser.StarExpr:
	case *sqlparser.AliasedExpr:
	case *sqlparser.Nextval:
	case sqlparser.Columns:
	case sqlparser.Partitions:
	case sqlparser.TableExprs:
	case *sqlparser.AliasedTableExpr:
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
	case sqlparser.TableName:
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
	case *sqlparser.ColIdent:
	case *sqlparser.TableIdent:
	}
	return nil
}
