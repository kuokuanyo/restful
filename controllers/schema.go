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
		if err != nil {
			message.Error = "Database information error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			sql = fmt.Sprintf(`drop table %s`, tablename)
		case "mssql":
			sql = fmt.Sprintf(`drop table %s.dbo.%s`, information.DBName, tablename)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = "drop schema error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully")
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
			description model.Description
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
		if err != nil {
			message.Error = "Database information error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			sql = fmt.Sprintf(`alter table %s %s`, tablename, description.Condition)
		case "mssql":
			sql = fmt.Sprintf(`alter table %s.dbo.%s %s`, information.DBName, tablename, description.Condition)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = "Update information of schema error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully")
	}
}

//CreateSchema :Add a new schema
//@Summary Add a new schema
//@Tags Schema
//@Accept json
//@Produce json
//@Param description body models.SchemaDescription true "description of table"
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
			description model.Description
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
		if err != nil {
			message.Error = "Database information error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		switch strings.ToLower(information.DBType) {
		case "mysql":
			sql = fmt.Sprintf(`create table %s(%s)`, tablename, description.Condition)
		case "mssql":
			sql = fmt.Sprintf(`create table %s.dbo.%s(%s)`, information.DBName, tablename, description.Condition)
		}
		if err = repo.Exec(DB, sql); err != nil {
			message.Error = "Add a new schema error"
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		utils.SendSuccess(w, "Successfully")
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
			fields      []model.FieldStructure
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
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sql := fmt.Sprintf("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME  from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'",
				information.DBName, tablename)
			rows, err = repo.Raw(DB, sql)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			if err != nil {
				message.Error = "Database information error"
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			sql := fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'`,
				information.DBName, tablename)
			rows, err = repo.Raw(DB, sql)
		}
		if err != nil {
			message.Error = "Get informations of field error."
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		for rows.Next() {
			var field model.FieldStructure
			rows.Scan(&field.TableCatalog, &field.TableSchema, &field.TableName, &field.ColumnName, &field.OrdinalPosition,
				&field.ColumnDefault, &field.IsNullable, &field.DataType, &field.CharacterMaximumLength, &field.CharacterOctetLength,
				&field.NumericPrecision, &field.NumericScale, &field.DatetimePrecision, &field.CharacterSetName, &field.CollationName)
			fields = append(fields, field)
		}
		utils.SendSuccess(w, fields)
	}
}

//GetAllSchema :get all schemas
//@Summary get all schemas
//@Tags Schema
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Success 200 {object} models.SchemaDefinition "Successfully"
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
			schemas     []model.SchemaDefinition
			sql         string
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
			sql = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,'TABLE_TYPE' from INFORMATION_SCHEMA.tables where TABLE_SCHEMA='%s'`, information.DBName)
			Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mysql", Source) //connect db
		case "mssql":
			sql = fmt.Sprintf("SELECT * FROM %s.INFORMATION_SCHEMA.TABLES", information.DBName)
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUserName,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
		}
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
			var schema model.SchemaDefinition
			rows.Scan(&schema.TableCatalog, &schema.TableSchema, &schema.TableName, &schema.TableType)
			schemas = append(schemas, schema)
		}
		utils.SendSuccess(w, schemas)
	}
}
