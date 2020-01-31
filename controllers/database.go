package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/jinzhu/gorm"
	"golang.org/x/crypto/bcrypt"
)

//DeleteDB :delete information of database from table users by some conditions.
//@Summary delete information of database from table users by some conditions.
//@Tags DataBase
//@Accept json
//@Produce json
//@Param filter string array false "SQL-like filter to limit the records to retrieve.if no filter, all datas could delete."
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1 [delete]
func (c Controller) DeleteDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message model.Error
			filter  = r.URL.Query()["filter"]
			sql     string
			repo    repository.Repository
		)
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
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
					splitequal[1] = fmt.Sprintf(`"%s"`, splitequal[1])
					j := strings.Join(splitequal, "=")
					newfilter = append(newfilter, j)
				}
				j := strings.Join(newfilter, " and ")
				sql += fmt.Sprintf("delete from users where %s ", j)
			} else if strings.Contains(filter[0], "like") {
				sql += fmt.Sprintf("delete from users where %s ", filter[0])
			} else {
				split := strings.Split(filter[0], "=")
				split[1] = fmt.Sprintf(`"%s"`, split[1])
				j := strings.Join(split, "=")
				sql += fmt.Sprintf("delete from users where %s ", j)
			}
		} else {
			sql = "delete from users"
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = "Delete information from users error."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully delete information from users.")
	}
}

//UpdateDB :update alias of database from table users(only could update alias)
//@Summary update alias of database from table users(only could update alias)
//@Tags DataBase
//@Accept json
//@Produce json
//@Param old_alias query string false "old_alias"
//@Param old_alias query string false "new_alias"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1 [put]
func (c Controller) UpdateDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message  model.Error
			oldalias = r.URL.Query()["old_alias"]
			newalias = r.URL.Query()["new_alias"]
			repo     repository.Repository
		)
		if len(oldalias) > 0 && len(newalias) > 0 {
			sql := fmt.Sprintf(`update users set db_alias="%s" where db_alias="%s"`, newalias[0], oldalias[0])
			DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
			if err != nil {
				message.Error = "Connect mysql.user db error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			if err = repo.Exec(DB, sql); err != nil {
				message.Error = "Update db_alias error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			utils.SendSuccess(w, "Successfully update alias of database.")
		} else {
			utils.SendSuccess(w, "No execute update command.")
		}
	}
}

//GetAllDB :get all db_alias information from table users
//@Summary get all db_alias information from table users
//@Tags DataBase
//@Accept json
//@Produce json
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties."
//@Param filter query string false "SQL-like filter to limit the records to retrieve."
//@Param limit query integer false "Set to limit the filter results."
//@Param offset query integer false "Set to offset the filter results to a particular record count."
//@Param order query string false "SQL-like order containing field and direction for filter results."
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1 [get]
func (c Controller) GetAllDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB           *gorm.DB
			err          error
			informations []map[string]interface{}
			message      model.Error
			sql          string
			slicefields  []string
			fields       = r.URL.Query()["fields"]
			filter       = r.URL.Query()["filter"]
			order        = r.URL.Query()["order"]
			limit        = r.URL.Query()["limit"]
			offset       = r.URL.Query()["offset"]
			repo         repository.Repository
		)
		DB, err = repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(fields) > 0 {
			sql = fmt.Sprintf("select %s from users ", fields[0])
			slicefields = strings.Split(fields[0], ",")
		} else {
			slicefields = []string{"db_alias", "db_type", "db_username", "db_password",
				"db_host", "db_port", "db", "maxidle", "maxopen"}
			sql = fmt.Sprintf("select %s from users ", strings.Join(slicefields, ","))
		}
		if len(filter) > 0 {
			if strings.Contains(filter[0], " and ") {
				var newfilter []string
				split := strings.Split(filter[0], " and ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`"%s"`, splitequal[1])
					j := strings.Join(splitequal, "=")
					newfilter = append(newfilter, j)
				}
				j := strings.Join(newfilter, " and ")
				sql += fmt.Sprintf("where %s ", j)
			} else if strings.Contains(filter[0], "like") {
				sql += fmt.Sprintf("where %s ", filter[0])
			} else {
				split := strings.Split(filter[0], "=")
				split[1] = fmt.Sprintf(`"%s"`, split[1])
				j := strings.Join(split, "=")
				sql += fmt.Sprintf("where %s ", j)
			}
		}
		if len(limit) > 0 {
			sql += fmt.Sprintf("limit %s ", limit[0])
		}
		if len(offset) > 0 {
			sql += fmt.Sprintf("offset %s ", offset[0])
		}
		if len(order) > 0 {
			sql += fmt.Sprintf("order by %s ", order[0])
		}
		var (
			value     = make([]string, len(slicefields))
			valuePtrs = make([]interface{}, len(slicefields)) //scan parameter need pointer
		)
		for i := 0; i < len(slicefields); i++ {
			valuePtrs[i] = &value[i] //scan parameter need pointer
		}
		rows, err := repo.Raw(DB, sql)
		if err != nil {
			message.Error = "get datas from table of users failed"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		for rows.Next() {
			information := make(map[string]interface{})
			if err = rows.Scan(valuePtrs...); err != nil {
				message.Error = "scan data failed"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for i := 0; i < len(slicefields); i++ {
				information[slicefields[i]] = value[i]
			}
			informations = append(informations, information)
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
//@Router /v1 [post]
func (c Controller) ConnectDB() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			information model.DBInformation
			message     model.Error
			err         error
			repo        repository.Repository
		)
		json.NewDecoder(r.Body).Decode(&information) //decode dbinformation
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DB_UserName,
				information.DB_Password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DB_UserName,
				information.DB_Password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
		}
		//insert information to database(user.users)
		DB, err = repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//encrypt password
		//func GenerateFromPassword(password []byte, cost int) ([]byte, error)
		hash, err := bcrypt.GenerateFromPassword([]byte(information.DB_Password), 10)
		if err != nil {
			message.Error = "encrypt password error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		information.DB_Password = string(hash)
		insert := fmt.Sprintf(`insert into users values ("%s", "%s", "%s", "%s", "%s", "%s", "%s", %d, %d)`,
			information.DB_Alias, information.DB_Type, information.DB_UserName,
			information.DB_Password, information.DB_Host, information.DB_Port,
			information.DB, information.MaxIdle, information.MaxOpen)
		if err = repo.Exec(DB, insert); err != nil {
			message.Error = "insert value error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successful add information of engine to user of database")
	}
}
