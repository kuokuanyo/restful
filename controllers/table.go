package controllers

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

//GetAllData :Retrieve one or more records.
func (c Controller) GetAllData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message     model.Error
			params      = mux.Vars(r)
			password    = r.URL.Query()["db_password"][0]
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			fields      = r.URL.Query()["fields"]
			information model.DBInformation
			repo        repository.Repository
			sqlorder    string
			slicefields []string
			rows        *sql.Rows
		)
		if password == "" {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//get informations from db_alias
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RawOneData(DB, fmt.Sprintf(`select * from users where db_alias="%s"`, dbalias))
		//scan information
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = "Error password."
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
		}
		if len(fields) > 0 {
			sqlorder = fmt.Sprintf("select %s from %s ", fields[0], tablename)
			slicefields = strings.Split(fields[0], ",")
		} else {
			sqlorder = fmt.Sprintln("select * from users ")
		}
		if err != nil {
			message.Error = "Database information error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
	}
}
