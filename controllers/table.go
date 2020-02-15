package controllers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jinzhu/gorm"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

//DeleteData :Delete one or more records.
//@Summary Delete one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param filter query array false "SQL-like filter to delete records."
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [delete]
func (c Controller) DeleteData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			passwords   = r.URL.Query()["db_password"]
			//related = r.URL.Query()["related"]
			filter   = r.URL.Query()["filter"]
			password string
			sqlorder string
			err      error
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`use %s; select * from engines where db_alias='%s'`, Storing.DBName, dbalias))
		}
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sqlorder = fmt.Sprintf(`delete from %s `, tablename)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			sqlorder = fmt.Sprintf(`use %s; delete from %s `, information.DBName, tablename)
		}
		if len(filter) > 0 {
			if strings.Contains(filter[0], " and ") {
				var slicefilter []string
				split := strings.Split(filter[0], " and ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				j := strings.Join(slicefilter, " and ")
				sqlorder += fmt.Sprintf(`where %s`, j)
			} else if strings.Contains(filter[0], " or ") {
				var slicefilter []string
				split := strings.Split(filter[0], " or ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[i])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				j := strings.Join(slicefilter, " or ")
				sqlorder += fmt.Sprintf("where %s", j)
			} else if strings.Contains(filter[0], " like ") {
				sqlorder += fmt.Sprintf("where %s ", filter[0])
			} else {
				split := strings.Split(filter[0], "=")
				split[1] = fmt.Sprintf(`'%s'`, split[1])
				j := strings.Join(split, "=")
				sqlorder += fmt.Sprintf("where %s ", j)
			}
		}
		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully.")
	}
}

//UpdateData :Update (replace) one or more records.
//@Summary Update (replace) one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param filter query array true "SQL-like filter to update records"
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Param condition body models.Description true "condition of Updating"
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [put]
func (c Controller) UpdateData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			description model.Description
			repo        repository.Repository
			message     model.Error
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			filter      = r.URL.Query()["filter"]
			//related = r.URL.Query()["related"]
			passwords = r.URL.Query()["db_password"]
			password  string
			sqlorder  string
			err       error
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
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
		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`use %s; select * from engines where db_alias='%s'`, Storing.DBName, dbalias))
		}
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sqlorder = fmt.Sprintf(`update %s %s `, tablename, description.Condition)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sqlorder = fmt.Sprintf(`use %s; update %s %s `, information.DBName, tablename, description.Condition)
		}
		if len(filter) > 0 {
			if strings.Contains(filter[0], " and ") {
				var slicefilter []string
				split := strings.Split(filter[0], " and ")
				for i := 0; i < len(split); i++ {
					s := split[i]
					splitequal := strings.Split(s, "=")
					splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
					j := strings.Join(splitequal, "=")
					slicefilter = append(slicefilter, j)
				}
				j := strings.Join(slicefilter, " and ")
				sqlorder += fmt.Sprintf("where %s", j)
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
				sqlorder += fmt.Sprintf("where %s", j)
			} else if strings.Contains(filter[0], " like ") {
				sqlorder += fmt.Sprintf("where %s ", filter[0])
			} else {
				split := strings.Split(filter[0], "=")
				split[1] = fmt.Sprintf(`'%s'`, split[1])
				j := strings.Join(split, "=")
				sqlorder += fmt.Sprintf("where %s", j)
			}
		}
		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully.")
	}
}

//AddData :Create one or more records.
//@Summary Create one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param fields query array true "set fields for adding value"
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Param value body models.InsertValue true "Insert value for adding"
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [post]
func (c Controller) AddData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			insertvalue model.InsertValue
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			fields      = r.URL.Query()["fields"]
			field       string
			//related     = r.URL.Query()["related"]
			passwords = r.URL.Query()["db_password"]
			password  string
			sqlorder  string
			err       error
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decode
		json.NewDecoder(r.Body).Decode(&insertvalue)
		if insertvalue.Value == "" {
			message.Error = "Required value for adding"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(fields) > 0 {
			field = fields[0]
			splitvalue := strings.Split(insertvalue.Value, ",")
			splitfield := strings.Split(field, ",")
			if len(splitvalue) != len(splitfield) {
				message.Error = "Column count doesn't match value."
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
		} else {
			message.Error = "Must set fields for adding value"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`use %s; select * from engines where db_alias='%s'`, Storing.DBName, dbalias))
		}
		//scan information
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
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
		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`insert into %s(%s) values (%s)`, tablename, field, insertvalue.Value)
		case "mssql":
			sqlorder = fmt.Sprintf(`use %s; insert into %s(%s) values (%s)`, information.DBName, tablename, field, insertvalue.Value)
		}
		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully.")
	}
}

//GetAllData :Retrieve one or more records.
//@Summary Retrieve one or more records.
//@Tags Table
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param db_password query string true "database engine password"
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties."
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Param filter query string false "SQL-like filter to limit the records to retrieve."
//@Param limit query integer false "Set to limit the filter results."
//@Param offset query integer false "Set to offset the filter results to a particular record count."
//@Param order query string false "SQL-like order containing field and direction for filter results."
//@Param group query string false "Comma-delimited list of the fields used for grouping of filter results."
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name} [get]
func (c Controller) GetAllData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			passwords   = r.URL.Query()["db_password"]
			fields      = r.URL.Query()["fields"]
			relateds    = r.URL.Query()["related"]
			filter      = r.URL.Query()["filter"]
			order       = r.URL.Query()["order"]
			limit       = r.URL.Query()["limit"]
			offset      = r.URL.Query()["offset"]
			fetch       = r.URL.Query()["fetch"]
			group       = r.URL.Query()["group"]
			password    string
			related     string
			sqlorder    string
			slicefields []string
			coltype     []string
			datas       []map[string]interface{}
			err         error
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Required password."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`use %s; select * from engines where db_alias='%s'`, Storing.DBName, dbalias))
		}
		//scan information
		if err = row.Scan(&information.DBAlias, &information.DBType, &information.DBUsername,
			&information.DBPassword, &information.DBHost, &information.DBPort,
			&information.DBName, &information.Maxidle, &information.Maxopen); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DBPassword), []byte(password)); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DBType) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				sqlorder = fmt.Sprintf("select %s from %s.%s ", fields[0], information.DBName, tablename)
				for i := range slicefields {
					var datatype string
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					if err = row.Scan(&datatype); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else if len(fields) == 0 {
				sqlorder = fmt.Sprintf("select * from %s.%s ", information.DBName, tablename)
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
				defer rows.Close()
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
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(slicefields) == 0 && len(coltype) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
			if len(relateds) > 0 {

			}
			if len(filter) > 0 {
				if strings.Contains(filter[0], " and ") {
					var slicefilter []string
					split := strings.Split(filter[0], " and ")
					for i := 0; i < len(split); i++ {
						s := split[i]
						splitequal := strings.Split(s, "=")
						splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
						j := strings.Join(splitequal, "=")
						slicefilter = append(slicefilter, j)
					}
					j := strings.Join(slicefilter, " and ")
					sqlorder += fmt.Sprintf("where %s ", j)
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
					j := strings.Join(split, " or ")
					sqlorder += fmt.Sprintf("where %s", j)
				} else if strings.Contains(filter[0], "like") {
					sqlorder += fmt.Sprintf("where %s ", filter[0])
				} else {
					split := strings.Split(filter[0], "=")
					split[1] = fmt.Sprintf(`'%s'`, split[1])
					j := strings.Join(split, "=")
					sqlorder += fmt.Sprintf("where %s ", j)
				}
			}
			if len(group) > 0 {
				sqlorder += fmt.Sprintf("group by %s ", group[0])
			}
			if len(order) > 0 {
				sqlorder += fmt.Sprintf("order by %s ", order[0])
			}
			if len(limit) > 0 {
				sqlorder += fmt.Sprintf("limit %s ", limit[0])
			}
			if len(offset) > 0 {
				if len(limit) > 0 {
					sqlorder += fmt.Sprintf("offset %s ", offset[0])
				} else if len(limit) == 0 {
					message.Error = "the offset parameter must have the parameter of limit"
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sqlorder = "select "
			if len(limit) > 0 {
				sqlorder = fmt.Sprintf("select top %s ", limit[0])
			}
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				sqlorder += fmt.Sprintf("%s from %s.dbo.%s ", fields[0], information.DBName, tablename)
				for i := range slicefields {
					var datatype string
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					if err = row.Scan(&datatype); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else if len(fields) == 0 {
				sqlorder += fmt.Sprintf("* from %s.dbo.%s ", information.DBName, tablename)
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' `, information.DBName, tablename))
				defer rows.Close()
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
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(slicefields) == 0 && len(coltype) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
			if len(filter) > 0 {
				if strings.Contains(filter[0], " and ") {
					var slicefilter []string
					split := strings.Split(filter[0], " and ")
					for i := 0; i < len(split); i++ {
						s := split[i]
						splitequal := strings.Split(s, "=")
						splitequal[1] = fmt.Sprintf(`'%s'`, splitequal[1])
						j := strings.Join(splitequal, "=")
						slicefilter = append(slicefilter, j)
					}
					j := strings.Join(slicefilter, " and ")
					sqlorder += fmt.Sprintf("where %s ", j)
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
					sqlorder += fmt.Sprintf("where %s", j)
				} else if strings.Contains(filter[0], "like") {
					sqlorder += fmt.Sprintf("where %s ", filter[0])
				} else {
					split := strings.Split(filter[0], "=")
					split[1] = fmt.Sprintf(`'%s'`, split[1])
					j := strings.Join(split, "=")
					sqlorder += fmt.Sprintf("where %s ", j)
				}
			}
			if len(group) > 0 {
				sqlorder += fmt.Sprintf("group by %s ", group[0])
			}
			if len(order) > 0 {
				sqlorder += fmt.Sprintf("order by %s ", order[0])
			}
			if len(offset) > 0 {
				if len(order) > 0 && len(fetch) > 0 {
					sqlorder += fmt.Sprintf("offset %s rows fetch next %s rows only ", offset[0], fetch[0])
				} else if len(order) == 0 || len(fetch) == 0 {
					message.Error = "the offset parameter must have the parameter of order and fetch."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}
		}
		var (
			value     = make([]string, len(slicefields))
			valuePtrs = make([]interface{}, len(slicefields)) //scan need pointer
		)
		for i := 0; i < len(slicefields); i++ {
			valuePtrs[i] = &value[i]
		}
		rows, err = repo.Rowmanydata(DB, sqlorder)
		defer rows.Close()
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		for rows.Next() {
			var data = make(map[string]interface{})
			rows.Scan(valuePtrs...)
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
			datas = append(datas, data)
		}
		if err = rows.Err(); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, datas)
	}
}
