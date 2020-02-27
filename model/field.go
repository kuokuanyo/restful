package model

//FieldStructure 資料表結構
type FieldStructure struct {
	TableCatalog           string
	TableSchema            string
	TableName              string
	ColumnName             string
	OrdinalPosition        string
	ColumnDefault          string
	IsNullable             string
	DataType               string
	CharacterMaximumLength string
	CharacterOctetLength   string
	NumericPrecision       string
	NumericScale           string
	DatetimePrecision      string
	CharacterSetName       string
	CollationName          string
}
