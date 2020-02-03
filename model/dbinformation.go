package model

//DBInformation :information of database
type DBInformation struct {
	DBAlias    string
	DBType     string
	DBUserName string
	DBPassword string
	DBHost     string
	DBPort     string
	DBName     string
	MaxIdle    int
	MaxOpen    int
}
