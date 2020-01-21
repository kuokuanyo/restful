package model

//DBInformation :information of database
type DBInformation struct {
	DB_Alias    string
	DB_Type     string
	DB_UserName string
	DB_Password string
	DB_Host     string
	DB_Port     string
	DB          string
	MaxIdle     int
	MaxOpen     int
}
