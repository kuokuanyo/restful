package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"golang.org/x/crypto/bcrypt"
)

//DeleteDB :delete information from database engines by some db_alias.
//@Summary delete information from database engines by some db_alias.
//@Tags DataBase
//@Accept json
//@Produce json
//@Param db_alias query string true "db_alias for deleting"
//@Success 200 {object} model.Engine "Successfully"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_engine [delete]
func (c Controller) DeleteDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message     model.Error
			repo        repository.Repository
			alias       = r.URL.Query()["db_alias"]
			information model.Engine
			sqlorder    string
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(alias) > 0 {
			switch strings.ToLower(Storing.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select * from engines where db_alias='%s'`, alias[0])
			case "mssql":
				sqlorder = fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
					Storing.DBName, alias[0])
			}

			row := repo.RowOneData(DBStoring, sqlorder)
			if err := row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
				&information.DBPassword, &information.DBHost, &information.DBPort,
				&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}

			switch strings.ToLower(Storing.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("delete from engines where db_alias='%s' ", alias[0])
			case "mssql":
				sqlorder = fmt.Sprintf("delete from %s.dbo.engines where db_alias='%s' ", Storing.DBName, alias[0])
			}
		} else {
			message.Error = "Must have parameter of db_alias."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if err := repo.Exec(DBStoring, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, information)
	}
}

//UpdateDB :update alias of database from database engines(only could update alias)
//@Summary update alias of database from database engines(only could update alias)
//@Tags DataBase
//@Accept json
//@Produce json
//@Param old_alias query string true "old_alias"
//@Param new_alias query string true "new_alias"
//@Success 200 {object} model.Engine "Successfully"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_engine [put]
func (c Controller) UpdateDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message     model.Error
			repo        repository.Repository
			oldalias    = r.URL.Query()["old_alias"]
			newalias    = r.URL.Query()["new_alias"]
			information model.Engine
			sqlorder    string
			err         error
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(oldalias) > 0 && len(newalias) > 0 {
			switch strings.ToLower(Storing.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`update engines set db_alias="%s" where db_alias="%s"`,
					newalias[0], oldalias[0])
			case "mssql":
				sqlorder = fmt.Sprintf(`update %s.dbo.engines set db_alias='%s' where db_alias='%s'`,
					Storing.DBName, newalias[0], oldalias[0])
			}

			if err = repo.Exec(DBStoring, sqlorder); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}

			switch strings.ToLower(Storing.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select * from engines where db_alias='%s'`, newalias[0])
			case "mssql":
				sqlorder = fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
					Storing.DBName, newalias[0])
			}

			row := repo.RowOneData(DBStoring, sqlorder)
			if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
				&information.DBPassword, &information.DBHost, &information.DBPort,
				&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}

			utils.SendSuccess(w, information)
		} else {
			message.Error = "Must have parameters of old_alias and new_alias"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
	}
}

//GetAllDB :get all information from database engines
//@Summary get all information from database engines
//@Tags DataBase
//@Accept json
//@Produce json
//@Success 200 {object} model.Engine "Successfully"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_engine [get]
func (c Controller) GetAllDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message      model.Error
			informations []model.Engine
			repo         repository.Repository
			err          error
			sqlorder     string
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintln("select * from engines")
		case "mssql":
			sqlorder = fmt.Sprintf(`select * from %s.dbo.engines`, Storing.DBName)
		}
		
		rows, err := repo.Rowmanydata(DBStoring, sqlorder)
		defer rows.Close()
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		for rows.Next() {
			var information model.Engine
			rows.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
				&information.DBPassword, &information.DBHost, &information.DBPort,
				&information.DBName, &information.Maxidle, &information.Maxopen)
			informations = append(informations, information)
		}
		if err = rows.Err(); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, informations)
	}
}

//ConnectDB :Add  information to database engine
//@Summary Add information to database engine
//@Tags DataBase
//@Accept json
//@Produce json
//@Param information body model.DBInformation true "information of database engine"
//@Success 200 {object} model.Engine "Successfully"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_engine [post]
func (c Controller) ConnectDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.Engine
			message     model.Error
			repo        repository.Repository
			err         error
			Source      string
			insert      string
		)

		if DBStoring == nil {
			message.Error = "Please connect the database for storing the engines."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		json.NewDecoder(r.Body).Decode(&information) //decode dbinformation

		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, information.DBPassword, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, information.DBPassword, information.DBHost,
				information.DBPort, information.DBName)
		}

		_, err = repo.ConnectDb(information.DBType, Source)
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		//encrypt password
		//func GenerateFromPassword(password []byte, cost int) ([]byte, error)
		hash, err := bcrypt.GenerateFromPassword([]byte(information.DBPassword), 10)
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		information.DBPassword = string(hash)

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			insert = fmt.Sprintf(`insert into engines values ("%s", "%s", "%s", "%s", "%s", "%s", "%s", %d, %d)`,
				information.DBAlias, information.DBType, information.DBUsername,
				information.DBPassword, information.DBHost, information.DBPort,
				information.DBName, information.Maxidle, information.Maxopen)
		case "mssql":
			insert = fmt.Sprintf(`insert into %s.dbo.engines values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d)`,
				Storing.DBName, information.DBAlias, information.DBType, information.DBUsername,
				information.DBPassword, information.DBHost, information.DBPort,
				information.DBName, information.Maxidle, information.Maxopen)
		}
		if err = repo.Exec(DBStoring, insert); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, information)
	}
}
