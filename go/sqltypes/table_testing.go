package sqltypes

import (
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// MakeTestFieldsWithTableName is similar to MakeTestResult.
func MakeTestFieldsWithTableName(names, types, tableName string) []*querypb.Field {
	n := split(names)
	t := split(types)
	var fields []*querypb.Field
	for i := range n {
		fields = append(fields, &querypb.Field{
			Table: tableName,
			Name:  n[i],
			Type:  querypb.Type(querypb.Type_value[strings.ToUpper(t[i])]),
		})
	}
	return fields
}
