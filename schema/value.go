package schema

import "github.com/xwb1989/sqlparser/dependency/querypb"

type Value struct {
	Type   querypb.Type
	Value  []byte
	Values []*Value
}

var Null = &Value{Type: querypb.Type_NULL_TYPE}
var True = &Value{Type: querypb.Type_INT64, Value: []byte("1")}
var False = &Value{Type: querypb.Type_INT64, Value: []byte("0")}
