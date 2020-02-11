package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"restful/model"
	"restful/repository"
	"restful/utils"
	"strings"

	"github.com/jinzhu/gorm"
)

//DBStoring :users of database engine
var DBStoring *gorm.DB

//Storing :struct
var Storing model.Storing

//Storing :create a table for recording information for database engine.
//@Summary create a table for recording information for database engine.
//@Tags Connect Database(Must be connected first)
//@Accept json
//@Produce json
//@Param information body models.DBinformation false "information of database engine for storing."
//@Success 200 {string} string "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1 [post]
func (c Controller) Storing() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message model.Error
			repo    repository.Repository
			Source  string
			err     error
		)
		json.NewDecoder(r.Body).Decode(&Storing)
		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				Storing.DBUsername, Storing.DBPassword,
				Storing.DBHost, Storing.DBPort, Storing.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				Storing.DBUsername, Storing.DBPassword,
				Storing.DBHost, Storing.DBPort, Storing.DBName)
		}
		DBStoring, err = repo.ConnectDb(Storing.DBType, Source)
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			if !DBStoring.HasTable(&model.Engine{}) {
				DBStoring.CreateTable(&model.Engine{})
			}
		case "mssql":
			err = repo.Exec(DBStoring,
				fmt.Sprintf(`use %s; IF NOT EXISTS (SELECT * FROM kuo.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'engines') CREATE TABLE engines (db_alias VARCHAR(20) PRIMARY KEY,db_type VARCHAR(10),db_username VARCHAR(20),db_password VARCHAR(200),db_host VARCHAR(20),db_port VARCHAR(10),db VARCHAR(20),maxidle TINYINT,maxopen TINYINT)`, Storing.DBName))
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
		}
		utils.SendSuccess(w, "Successfully.")
	}
}
