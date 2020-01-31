package controllers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"restful/model"
	"restful/repository"
	"restful/utils"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
)

//DeleteSchema :drop schema
//@Summary drop schema
//@Tags Schema
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "table name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [delete]
func (c Controller) DeleteSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			sql         string
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RawOneData(DB, fmt.Sprintf(`select * from users where db_alias="%s"`, dbalias))
		//scan information
		row.Scan(&information.DB_Alias, &information.DB_Type, &information.DB_UserName,
			&information.DB_Password, &information.DB_Host, &information.DB_Port,
			&information.DB, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DB_Password), []byte(password)); err != nil {
			message.Error = "Error password."
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mssql", Source)
		}
		if err != nil {
			message.Error = "Database information error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			sql = fmt.Sprintf(`drop table %s`, tablename)
		case "mssql":
			sql = fmt.Sprintf(`drop table %s.dbo.%s`, information.DB, tablename)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = "drop schema error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully drop schema")
	}
}

//UpdateSchema :alter information of schema
//@Summary alter information of schema
//@Tags Schema
//@Accept json
//@Produce json
//@Param description body models.SchemaDescription true "Update the description of schema"
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "table name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [put]
func (c Controller) UpdateSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			description model.SchemaDescription
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			sql         string
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RawOneData(DB, fmt.Sprintf(`select * from users where db_alias="%s"`, dbalias))
		//scan information
		row.Scan(&information.DB_Alias, &information.DB_Type, &information.DB_UserName,
			&information.DB_Password, &information.DB_Host, &information.DB_Port,
			&information.DB, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DB_Password), []byte(password)); err != nil {
			message.Error = "Error password."
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mssql", Source)
		}
		if err != nil {
			message.Error = "Database information error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			sql = fmt.Sprintf(`alter table %s %s`, tablename, description.Condition)
		case "mssql":
			sql = fmt.Sprintf(`alter table %s.dbo.%s %s`, information.DB, tablename, description.Condition)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = "Update information of schema error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully Update schema")
	}
}

//CreateSchema :Add a new schema
//@Summary Add a new schema
//@Tags Schema
//@Accept json
//@Produce json
//@Param description body models.SchemaDescription true "description of field"
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "table name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [post]
func (c Controller) CreateSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			description model.SchemaDescription
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			sql         string
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
			message.Error = "Require password"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)
		//get informations from db_alias
		DB, err := repo.ConnectDb("mysql", "kuokuanyo:asdf4440@tcp(127.0.0.1:3306)/user")
		if err != nil {
			message.Error = "Connect mysql.user db error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		row := repo.RawOneData(DB, fmt.Sprintf(`select * from users where db_alias="%s"`, dbalias))
		//scan information
		row.Scan(&information.DB_Alias, &information.DB_Type, &information.DB_UserName,
			&information.DB_Password, &information.DB_Host, &information.DB_Port,
			&information.DB, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DB_Password), []byte(password)); err != nil {
			message.Error = "Error password."
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mssql", Source)
		}
		if err != nil {
			message.Error = "Database information error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			sql = fmt.Sprintf(`create table %s(%s)`, tablename, description.Condition)
		case "mssql":
			sql = fmt.Sprintf(`create table %s.dbo.%s(%s)`, information.DB, tablename, description.Condition)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = "Add a new schema error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully add a new schema")
	}
}

//GetAllFields :get all informaiton of field
//@Summary get all informaiton of field
//@Tags Schema
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param table_name path string true "table name"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.FieldStructure "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [get]
func (c Controller) GetAllFields() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			information model.DBInformation
			message     model.Error
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			password    string
			passwords   = r.URL.Query()["db_password"]
			repo        repository.Repository
			structures  []model.FieldStructure
			rows        *sql.Rows
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
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
		row.Scan(&information.DB_Alias, &information.DB_Type, &information.DB_UserName,
			&information.DB_Password, &information.DB_Host, &information.DB_Port,
			&information.DB, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DB_Password), []byte(password)); err != nil {
			message.Error = "Error password."
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sql := fmt.Sprintf("select COLUMN_NAME, DATA_TYPE, IS_NULLABLE,COLUMN_DEFAULT from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s' and TABLE_SCHEMA='%s'",
				tablename, information.DB)
			rows, err = repo.Raw(DB, sql)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sql := fmt.Sprintf(`select COLUMN_NAME, DATA_TYPE, IS_NULLABLE,COLUMN_DEFAULT from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'`,
				information.DB, tablename)
			rows, err = repo.Raw(DB, sql)
		}
		if err != nil {
			message.Error = "Get informations of field error."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		for rows.Next() {
			var field model.FieldStructure
			rows.Scan(&field.Field, &field.Type, &field.Null, &field.Default)
			structures = append(structures, field)
		}
		utils.SendSuccess(w, structures)
	}
}

//GetAllSchema :get all schemas
//@Summary get all schemas
//@Tags Schema
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.object "Successfully"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias} [get]
func (c Controller) GetAllSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			message     model.Error
			params      = mux.Vars(r)
			password    string
			passwords   = r.URL.Query()["db_password"]
			dbalias     = params["db_alias"]
			information model.DBInformation
			repo        repository.Repository
			tables      []string
		)
		if len(passwords) > 0 {
			password = passwords[0]
		} else {
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
		row.Scan(&information.DB_Alias, &information.DB_Type, &information.DB_UserName,
			&information.DB_Password, &information.DB_Host, &information.DB_Port,
			&information.DB, &information.MaxIdle, &information.MaxOpen)
		//decrypt password
		if err = bcrypt.CompareHashAndPassword([]byte(information.DB_Password), []byte(password)); err != nil {
			message.Error = "Error password."
			utils.SendError(w, http.StatusUnauthorized, message)
			return
		}
		//identify db_type
		switch strings.ToLower(information.DB_Type) {
		case "mysql":
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			//Pluck:find one column
			//func (s *DB) Pluck(column string, value interface{}) *DB
			if err = repo.GetAlltables(DB, &tables); err != nil {
				message.Error = "Get all tables error."
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
		case "mssql":
			sql := fmt.Sprintf("SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES", information.DB)
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DB_UserName,
				password,
				information.DB_Host,
				information.DB_Port,
				information.DB)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			rows, err := repo.Raw(DB, sql)
			if err != nil {
				message.Error = "Get all tables error."
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for rows.Next() {
				var table string
				rows.Scan(&table)
				tables = append(tables, table)
			}
		}
		utils.SendSuccess(w, tables)
	}
}
