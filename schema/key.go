package schema

import "database/sql/driver"

type Key struct {
	Qualifier string
	Value     driver.Value
}
