package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"

	"restful/model"
	"restful/repository"
	"restful/utils"
)

//UpdateDataByid :Replace the content of one record by identifier.
func (c Controller) UpdateDataByid() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			description model.Description
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			id, _       = strconv.Atoi(params["id"])
			//related = r.URL.Query()["related"]
			password = r.URL.Query()["db_password"][0]
			sqlorder string
		)
		if password == "" {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decode
		json.NewDecoder(r.Body).Decode(&description)
		if description.Condition == "" {
			message.Error = "Required condition for updating."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RawOneData(DB, fmt.Sprintf(`select * from users where db_alias='%s'`, dbalias))
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			sqlorder = fmt.Sprintf(`update %s %s where id=%d`, tablename, description.Condition, id)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			sqlorder = fmt.Sprintf(`update %s.dbo.%s %s where id=%d `, information.DBName, tablename, description.Condition, id)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		
	}
}

//GetDataByid :Retrieve one record by identifier.
//@Summary Retrieve one record by identifier.
//@Tags Table By ID
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param id path int true "Identifier of the record to retrieve."
//@Param db_password query string true "database engine password"
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties."
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name}/{id} [get]
func (c Controller) GetDataByid() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			id, _       = strconv.Atoi(params["id"])
			password    = r.URL.Query()["db_password"][0]
			fields      = r.URL.Query()["fields"]
			//related = r.URL.Query()["related"]
			sqlorder    string
			slicefields []string
			coltype     []string
			data        = make(map[string]interface{})
		)
		if password == "" {
			message.Error = "Required password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RawOneData(DB, fmt.Sprintf(`select * from users where db_alias='%s'`, dbalias))
		row.Scan(&information.DBAlias, &information.DBType, &information.DBUserName,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.MaxIdle, &information.MaxOpen)
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = "Error password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(fields) > 0 {
			slicefields = strings.Split(fields[0], ",")
			for i := range slicefields {
				var datatype string
				row = repo.RawOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
				row.Scan(&datatype)
				if datatype == "" {
					coltype = append(coltype, "varchar")
				} else {
					coltype = append(coltype, datatype)
				}
			}
			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, fields[0], tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`select %s from %s.dbo.%s where id=%d`, fields[0], information.DBName, tablename, id)
			}
		} else {
			rows, err := repo.Raw(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var table string
				var datatype string
				rows.Scan(&table, &datatype)
				slicefields = append(slicefields, table)
				coltype = append(coltype, datatype)
			}
			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select * from %s where id=%d`, tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`select * from %s.dbo.%s where id=%d`, information.DBName, tablename, id)
			}
		}
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
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		var (
			value     = make([]string, len(slicefields))
			valuePtrs = make([]interface{}, len(slicefields)) //scan need pointer
		)
		for i := 0; i < len(slicefields); i++ {
			valuePtrs[i] = &value[i]
		}
		row = repo.RawOneData(DB, sqlorder)
		row.Scan(valuePtrs...)
		for i := range slicefields {
			if strings.Contains(coltype[i], "varchar") {
				data[slicefields[i]] = value[i]
			} else if strings.Contains(coltype[i], "int") {
				data[slicefields[i]], err = strconv.Atoi(value[i])
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
		}
		utils.SendSuccess(w, data)
	}
}
