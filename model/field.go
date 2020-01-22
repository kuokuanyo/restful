package model

import "database/sql"

//FieldStructure 資料表結構
type FieldStructure struct {
	Field   string
	Type    string
	Null    string
	Default sql.NullString
}
