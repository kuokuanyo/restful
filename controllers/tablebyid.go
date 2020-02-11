package controllers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jinzhu/gorm"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"

	"restful/model"
	"restful/repository"
	"restful/utils"
)

//DeleteDataByid :Delete one record by identifier.
//@Summary Delete one record by identifier.
//@Tags Table By ID(id is primary key)
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param id path int true "Identifier of the record to retrieve."
//@Param db_password query string true "database engine password"
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties."
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name}/{id} [delete]
func (c Controller) DeleteDataByid() http.HandlerFunc {
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
			id, _       = strconv.Atoi(params["id"])
			password    = r.URL.Query()["db_password"][0]
			fields      = r.URL.Query()["fields"]
			//related = r.URL.Query()["related"]
			sqlorder       string
			deletesqlorder string
			slicefields    []string
			coltype        []string
			data           = make(map[string]interface{})
			err            error
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if password == "" {
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
			DB, err = repo.ConnectDb("mysql", Source)
			deletesqlorder = fmt.Sprintf(`delete from %s where id=%d`, tablename, id)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			deletesqlorder = fmt.Sprintf(`use %s; delete from %s where id=%d`, information.DBName, tablename, id)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if len(fields) > 0 {
			slicefields = strings.Split(fields[0], ",")
			for i := range slicefields {
				var datatype string
				switch strings.ToLower(information.DBType) {
				case "mysql":
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, fields[0], tablename, id)
				case "mssql":
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					sqlorder = fmt.Sprintf(`use %s; select %s from %s where id=%d`, information.DBName, fields[0], tablename, id)
				}
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
			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, fields[0], tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`use %s; select %s from %s where id=%d`, information.DBName, fields[0], tablename, id)
			}
		} else {
			switch strings.ToLower(information.DBType) {
			case "mysql":
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
				sqlorder = fmt.Sprintf(`select * from %s where id=%d`, tablename, id)
			case "mssql":
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' `, information.DBName, tablename))
				sqlorder = fmt.Sprintf(`use %s; select * from %s where id=%d`, information.DBName, tablename, id)
			}
			defer rows.Close()
			if err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var table, datatype string
				rows.Scan(&table, &datatype)
				slicefields = append(slicefields, table)
				coltype = append(coltype, datatype)
			}
			if err = rows.Err(); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
		}
		var (
			value     = make([]string, len(slicefields))
			valuePtrs = make([]interface{}, len(slicefields))
		)
		for i := 0; i < len(slicefields); i++ {
			valuePtrs[i] = &value[i]
		}
		row = repo.RowOneData(DB, sqlorder)
		if err = row.Scan(valuePtrs...); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
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
		if err = repo.Exec(DB, deletesqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, data)
	}
}

//UpdateDataByid :Replace the content of one record by identifier.
//@Summary Replace the content of one record by identifier.
//@Tags Table By ID(id is primary key)
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param id path int true "Identifier of the record to retrieve."
//@Param db_password query string true "database engine password"
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties."
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Param condition body models.Description true "condition of Updating"
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name}/{id} [put]
func (c Controller) UpdateDataByid() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			description model.Description
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			tablename   = params["table_name"]
			dbalias     = params["db_alias"]
			id, _       = strconv.Atoi(params["id"])
			//related = r.URL.Query()["related"]
			password    = r.URL.Query()["db_password"][0]
			fields      = r.URL.Query()["fields"]
			sqlorder    string
			slicefields []string
			coltype     []string
			data        = make(map[string]interface{})
			rows        *sql.Rows
			err         error
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
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
		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)
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
			sqlorder = fmt.Sprintf(`update %s %s where id=%d`, tablename, description.Condition, id)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			sqlorder = fmt.Sprintf(`use %s; update %s %s where id=%d `, information.DBName, tablename, description.Condition, id)
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
		if len(fields) > 0 {
			slicefields = strings.Split(fields[0], ",")
			for i := range slicefields {
				var datatype string
				switch strings.ToLower(information.DBType) {
				case "mysql":
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
				case "mssql":
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
				}
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
			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, fields[0], tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`use %s; select %s from %s where id=%d`, information.DBName, fields[0], tablename, id)
			}
		} else {
			switch strings.ToLower(information.DBType) {
			case "mysql":
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
				sqlorder = fmt.Sprintf(`select * from %s where id=%d`, tablename, id)
			case "mssql":
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' `, information.DBName, tablename))
				sqlorder = fmt.Sprintf(`use %s; select * from %s where id=%d`, information.DBName, tablename, id)
			}
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
		}
		var (
			value     = make([]string, len(slicefields))
			valuePtrs = make([]interface{}, len(slicefields)) //scan need pointer
		)
		for i := 0; i < len(slicefields); i++ {
			valuePtrs[i] = &value[i]
		}
		row = repo.RowOneData(DB, sqlorder)
		if err = row.Scan(valuePtrs...); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
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

//GetDataByid :Retrieve one record by identifier.
//@Summary Retrieve one record by identifier.
//@Tags Table By ID(id is primary key)
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "Name of the table to perform operations on."
//@Param id path int true "Identifier of the record to retrieve."
//@Param db_password query string true "database engine password"
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties."
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource."
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name}/{id} [get]
func (c Controller) GetDataByid() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
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
			rows        *sql.Rows
			err         error
		)
		if DBStoring == nil {
			message.Error = "Please connect the database for storing informations of engine."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		if password == "" {
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
		if len(fields) > 0 {
			slicefields = strings.Split(fields[0], ",")
			for i := range slicefields {
				var datatype string
				switch strings.ToLower(information.DBType) {
				case "mysql":
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
				case "mssql":
					row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s'`, information.DBName, tablename, slicefields[i]))
				}
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
			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, fields[0], tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`use %s; select %s from %s where id=%d`, information.DBName, fields[0], tablename, id)
			}
		} else {
			switch strings.ToLower(information.DBType) {
			case "mysql":
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, information.DBName, tablename))
				sqlorder = fmt.Sprintf(`select * from %s where id=%d`, tablename, id)
			case "mssql":
				rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s'`, information.DBName, tablename))
				sqlorder = fmt.Sprintf(`use %s; select * from %s where id=%d`, information.DBName, tablename, id)
			}
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
		var (
			value     = make([]string, len(slicefields))
			valuePtrs = make([]interface{}, len(slicefields)) //scan need pointer
		)
		for i := 0; i < len(slicefields); i++ {
			valuePtrs[i] = &value[i]
		}
		row = repo.RowOneData(DB, sqlorder)
		if err = row.Scan(valuePtrs...); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
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
