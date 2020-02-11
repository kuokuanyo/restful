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

//DeleteDB :delete information of database from table engines by some conditions.
//@Summary delete information of database from table engines by some conditions.
//@Tags DataBase
//@Accept json
//@Produce json
//@Param filter string array false "SQL-like filter to limit the records to retrieve.if no condition of filter, all datas could delete."
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_engine [delete]
func (c Controller) DeleteDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message model.Error
			repo    repository.Repository
			filter  = r.URL.Query()["filter"]
			sql     string
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(filter) > 0 {
			if strings.Contains(filter[0], " and ") {
				var newfilter []string
				split := strings.Split(filter[0], " and ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					newfilter = append(newfilter, j)
				}
				j := strings.Join(newfilter, " and ")
				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					sql += fmt.Sprintf("delete from engines where %s ", j)
				case "mssql":
					sql += fmt.Sprintf("use %s; delete from engines where %s ", Storing.DBName, j)
				}
			} else if strings.Contains(filter[0], " or ") {
				var slicefilter []string
				split := strings.Split(filter[0], " or ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				j := strings.Join(slicefilter, " or ")
				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					sql += fmt.Sprintf("delete from engines where %s ", j)
				case "mssql":
					sql += fmt.Sprintf("use %s; delete from engines where %s ", Storing.DBName, j)
				}
			} else if strings.Contains(filter[0], "like") {
				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					sql += fmt.Sprintf("delete from engines where %s ", filter[0])
				case "mssql":
					sql += fmt.Sprintf("use %s; delete from engines where %s ", Storing.DBName, filter[0])
				}
			} else {
				split := strings.Split(filter[0], "=")
				split[1] = fmt.Sprintf(`'%s'`, split[1])
				j := strings.Join(split, "=")
				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					sql += fmt.Sprintf("delete from engines where %s ", j)
				case "mssql":
					sql += fmt.Sprintf("use %s; delete from engines where %s ", Storing.DBName, j)
				}
			}
		} else {
			switch strings.ToLower(Storing.DBType) {
			case "mysql":
				sql = "delete from engines"
			case "mssql":
				sql = fmt.Sprintf("use %s; delete from engines", Storing.DBName)
			}
		}
		if err := repo.Exec(DBStoring, sql); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(filter) > 0 {
			utils.SendSuccess(w, "Successfully.")
		} else {
			utils.SendSuccess(w, "All records are deleted.")
		}
	}
}

//UpdateDB :update alias of database from table engines(only could update alias)
//@Summary update alias of database from table engines(only could update alias)
//@Tags DataBase
//@Accept json
//@Produce json
//@Param old_alias query string false "old_alias"
//@Param old_alias query string false "new_alias"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_engine [put]
func (c Controller) UpdateDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message  model.Error
			repo     repository.Repository
			oldalias = r.URL.Query()["old_alias"]
			newalias = r.URL.Query()["new_alias"]
			sqlorder string
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
				sqlorder = fmt.Sprintf(`use %s; update engines set db_alias='%s' where db_alias='%s'`,
					Storing.DBName, newalias[0], oldalias[0])
			}
			if err := repo.Exec(DBStoring, sqlorder); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			utils.SendSuccess(w, "Successfully.")
		} else {
			utils.SendSuccess(w, "No execute update command.")
		}
	}
}

//GetAllDB :get all db_alias information from table engines
//@Summary get all db_alias information from table engines
//@Tags DataBase
//@Accept json
//@Produce json
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
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
			sqlorder = fmt.Sprintf(`use %s; select * from engines`, Storing.DBName)
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

//ConnectDB :Add information of database to database engine
//@Summary Add information of database to database engine
//@Tags DataBase
//@Accept json
//@Produce json
//@Param information body models.DBInformation true "information of database"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
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
				information.DBUsername,
				information.DBPassword,
				information.DBHost,
				information.DBPort,
				information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				information.DBPassword,
				information.DBHost,
				information.DBPort,
				information.DBName)
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
			insert = fmt.Sprintf(`use %s; insert into engines values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d)`,
				Storing.DBName, information.DBAlias, information.DBType, information.DBUsername,
				information.DBPassword, information.DBHost, information.DBPort,
				information.DBName, information.Maxidle, information.Maxopen)
		}
		if err = repo.Exec(DBStoring, insert); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully.")
	}
}
