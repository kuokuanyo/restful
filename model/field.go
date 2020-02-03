package model

import "database/sql"

//FieldStructure 資料表結構
type FieldStructure struct {
	TableCatalog           string
	TableSchema            string
	TableName              string
	ColumnName             string
	OrdinalPosition        string
	ColumnDefault          sql.NullString
	IsNullable             string
	DataType               string
	CharacterMaximumLength sql.NullString
	CharacterOctetLength   sql.NullString
	NumericPrecision       sql.NullString
	NumericScale           sql.NullString
	DatetimePrecision      sql.NullString
	CharacterSetName       sql.NullString
	CollationName          sql.NullString
}
