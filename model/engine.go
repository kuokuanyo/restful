package model

//Engine :information of database
type Engine struct {
	DBAlias    string `gorm:"primary_key"`
	DBType     string
	DBUsername string
	DBPassword string
	DBHost     string
	DBPort     string
	DBName     string
	Maxidle    int
	Maxopen    int
}
