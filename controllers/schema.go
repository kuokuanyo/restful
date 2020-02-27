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
	"github.com/jinzhu/gorm"
	"golang.org/x/crypto/bcrypt"
)

//DeleteSchema :Drop the given table.
//@Summary Drop the given table.
//@Tags Schema
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [delete]
func (c Controller) DeleteSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			passwords   = r.URL.Query()["db_password"]
			fields      []model.FieldStructure
			Source      string
			password    string
			sqlorder    string
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
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`, Storing.DBName, dbalias))
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
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA,
			 TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			 IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			 NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME  
			 from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
				information.DBName, tablename)
		case "mssql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,
			NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'`,
				information.DBName, tablename)
		}
		
		rows, err = repo.Rowmanydata(DB, sqlorder)
		defer rows.Close()
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		for rows.Next() {
			var field model.FieldStructure
			rows.Scan(valuePtrs...)

			field.TableCatalog = value[0].String
			field.TableSchema = value[1].String
			field.TableName = value[2].String
			field.ColumnName = value[3].String
			field.OrdinalPosition = value[4].String
			if value[5].String == "" {
				field.ColumnDefault = "NULL"
			} else {
				field.ColumnDefault = value[5].String
			}
			field.IsNullable = value[6].String
			field.DataType = value[7].String
			if value[8].String == "" {
				field.CharacterMaximumLength = "NULL"
			} else {
				field.CharacterMaximumLength = value[8].String
			}
			if value[9].String == "" {
				field.CharacterOctetLength = "NULL"
			} else {
				field.CharacterOctetLength = value[9].String
			}
			if value[10].String == "" {
				field.NumericPrecision = "NULL"
			} else {
				field.NumericPrecision = value[10].String
			}
			if value[11].String == "" {
				field.NumericScale = "NULL"
			} else {
				field.NumericScale = value[11].String
			}
			if value[12].String == "" {
				field.DatetimePrecision = "NULL"
			} else {
				field.DatetimePrecision = value[12].String
			}
			if value[13].String == "" {
				field.CharacterSetName = "NULL"
			} else {
				field.CharacterSetName = value[13].String
			}
			if value[14].String == "" {
				field.CollationName = "NULL"
			} else {
				field.CollationName = value[14].String
			}

			fields = append(fields, field)
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`drop table %s`, tablename)
		case "mssql":
			sqlorder = fmt.Sprintf(`drop table %s.dbo.%s`, information.DBName, tablename)
		}
		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, fields)
	}
}

//UpdateSchema :Update table fields with the given properties.
//@Summary Update table fields with the given properties.
//@Tags Schema
//@Accept json
//@Produce json
//@Param condition body model.SchemaDescription true "Update the description of schema"
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [put]
func (c Controller) UpdateSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			description model.Description
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			passwords   = r.URL.Query()["db_password"]
			fields      []model.FieldStructure
			Source      string
			password    string
			sqlorder    string
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

		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`,
				dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring,
				fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
					Storing.DBName, dbalias))
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
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`alter table %s %s`,
				tablename, description.Condition)
		case "mssql":
			sqlorder = fmt.Sprintf(`alter table %s.dbo.%s %s`,
				information.DBName, tablename, description.Condition)
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA,
			 TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			 IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			 NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME  
			 from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
				information.DBName, tablename)
		case "mssql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,
			NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'`,
				information.DBName, tablename)
		}

		rows, err = repo.Rowmanydata(DB, sqlorder)
		defer rows.Close()
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		for rows.Next() {
			var field model.FieldStructure
			rows.Scan(valuePtrs...)

			field.TableCatalog = value[0].String
			field.TableSchema = value[1].String
			field.TableName = value[2].String
			field.ColumnName = value[3].String
			field.OrdinalPosition = value[4].String
			if value[5].String == "" {
				field.ColumnDefault = "NULL"
			} else {
				field.ColumnDefault = value[5].String
			}
			field.IsNullable = value[6].String
			field.DataType = value[7].String
			if value[8].String == "" {
				field.CharacterMaximumLength = "NULL"
			} else {
				field.CharacterMaximumLength = value[8].String
			}
			if value[9].String == "" {
				field.CharacterOctetLength = "NULL"
			} else {
				field.CharacterOctetLength = value[9].String
			}
			if value[10].String == "" {
				field.NumericPrecision = "NULL"
			} else {
				field.NumericPrecision = value[10].String
			}
			if value[11].String == "" {
				field.NumericScale = "NULL"
			} else {
				field.NumericScale = value[11].String
			}
			if value[12].String == "" {
				field.DatetimePrecision = "NULL"
			} else {
				field.DatetimePrecision = value[12].String
			}
			if value[13].String == "" {
				field.CharacterSetName = "NULL"
			} else {
				field.CharacterSetName = value[13].String
			}
			if value[14].String == "" {
				field.CollationName = "NULL"
			} else {
				field.CollationName = value[14].String
			}

			fields = append(fields, field)
		}
		if err = rows.Err(); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, fields)
	}
}

//CreateSchema :Create a table with the given properties and fields.
//@Summary Create a table with the given properties and fields.
//@Tags Schema
//@Accept json
//@Produce json
//@Param condition body model.SchemaDescription true "description of table"
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [post]
func (c Controller) CreateSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			rows        *sql.Rows
			information model.Engine
			description model.Description
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			tablename   = params["table_name"]
			passwords   = r.URL.Query()["db_password"]
			fields      []model.FieldStructure
			Source      string
			password    string
			sqlorder    string
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

		//decode condition of create table
		json.NewDecoder(r.Body).Decode(&description)

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`, Storing.DBName, dbalias))
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
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`create table %s(%s)`, tablename, description.Condition)
		case "mssql":
			sqlorder = fmt.Sprintf(`create table %s.dbo.%s(%s)`, information.DBName, tablename, description.Condition)
		}

		if err = repo.Exec(DB, sqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		switch strings.ToLower(information.DBType) {
		case "mysql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA,
			 TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			 IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			 NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME  
			 from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
				information.DBName, tablename)
		case "mssql":
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,
			NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'`,
				information.DBName, tablename)
		}
		rows, err = repo.Rowmanydata(DB, sqlorder)
		defer rows.Close()
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		for rows.Next() {
			var field model.FieldStructure
			rows.Scan(valuePtrs...)

			field.TableCatalog = value[0].String
			field.TableSchema = value[1].String
			field.TableName = value[2].String
			field.ColumnName = value[3].String
			field.OrdinalPosition = value[4].String
			if value[5].String == "" {
				field.ColumnDefault = "NULL"
			} else {
				field.ColumnDefault = value[5].String
			}
			field.IsNullable = value[6].String
			field.DataType = value[7].String
			if value[8].String == "" {
				field.CharacterMaximumLength = "NULL"
			} else {
				field.CharacterMaximumLength = value[8].String
			}
			if value[9].String == "" {
				field.CharacterOctetLength = "NULL"
			} else {
				field.CharacterOctetLength = value[9].String
			}
			if value[10].String == "" {
				field.NumericPrecision = "NULL"
			} else {
				field.NumericPrecision = value[10].String
			}
			if value[11].String == "" {
				field.NumericScale = "NULL"
			} else {
				field.NumericScale = value[11].String
			}
			if value[12].String == "" {
				field.DatetimePrecision = "NULL"
			} else {
				field.DatetimePrecision = value[12].String
			}
			if value[13].String == "" {
				field.CharacterSetName = "NULL"
			} else {
				field.CharacterSetName = value[13].String
			}
			if value[14].String == "" {
				field.CollationName = "NULL"
			} else {
				field.CollationName = value[14].String
			}

			fields = append(fields, field)
		}
		if err = rows.Err(); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, fields)
	}
}

//GetAllFields :Retrieve table field definitions for the given table.
//@Summary Retrieve table field definitions for the given table.
//@Tags Schema
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Param table_name path string true "Name of the table to perform operations on."
//@Success 200 {object} model.FieldStructure "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias}/{table_name} [get]
func (c Controller) GetAllFields() http.HandlerFunc {
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
			fields      []model.FieldStructure
			rows        *sql.Rows
			sqlorder    string
			password    string
			Source      string
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
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`,
				dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
				Storing.DBName, dbalias))
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
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA,
			 TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			 IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, 
			 NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME  
			 from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
				information.DBName, tablename)
		case "mssql":
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, 
			IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH,
			NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, 
			COLLATION_NAME from %s.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'`,
				information.DBName, tablename)
		}
		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		rows, err = repo.Rowmanydata(DB, sqlorder)
		defer rows.Close()
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		var (
			value     = make([]sql.NullString, 15)
			valuePtrs = make([]interface{}, 15)
		)
		for i := 0; i < 15; i++ {
			valuePtrs[i] = &value[i]
		}

		for rows.Next() {
			var field model.FieldStructure
			rows.Scan(valuePtrs...)

			field.TableCatalog = value[0].String
			field.TableSchema = value[1].String
			field.TableName = value[2].String
			field.ColumnName = value[3].String
			field.OrdinalPosition = value[4].String
			if value[5].String == "" {
				field.ColumnDefault = "NULL"
			} else {
				field.ColumnDefault = value[5].String
			}
			field.IsNullable = value[6].String
			field.DataType = value[7].String
			if value[8].String == "" {
				field.CharacterMaximumLength = "NULL"
			} else {
				field.CharacterMaximumLength = value[8].String
			}
			if value[9].String == "" {
				field.CharacterOctetLength = "NULL"
			} else {
				field.CharacterOctetLength = value[9].String
			}
			if value[10].String == "" {
				field.NumericPrecision = "NULL"
			} else {
				field.NumericPrecision = value[10].String
			}
			if value[11].String == "" {
				field.NumericScale = "NULL"
			} else {
				field.NumericScale = value[11].String
			}
			if value[12].String == "" {
				field.DatetimePrecision = "NULL"
			} else {
				field.DatetimePrecision = value[12].String
			}
			if value[13].String == "" {
				field.CharacterSetName = "NULL"
			} else {
				field.CharacterSetName = value[13].String
			}
			if value[14].String == "" {
				field.CollationName = "NULL"
			} else {
				field.CollationName = value[14].String
			}

			fields = append(fields, field)
		}
		if err = rows.Err(); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, fields)
	}
}

//GetAllSchema :Retrieve one or more DbSchema.
//@Summary Retrieve one or more DbSchema.
//@Tags Schema
//@Accept json
//@Produce json
//@Param db_alias path string true "database engine alias"
//@Param db_password query string true "database engine password"
//@Success 200 {object} model.SchemaDefinition "Successfully"
//@Failure 401 {object} model.Error "Unauthorized"
//@Failure 500 {object} model.Error "Internal Server Error"
//@Router /v1/_schema/{db_alias} [get]
func (c Controller) GetAllSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB          *gorm.DB
			row         *sql.Row
			information model.Engine
			message     model.Error
			repo        repository.Repository
			params      = mux.Vars(r)
			dbalias     = params["db_alias"]
			password    = r.URL.Query()["db_password"][0]
			schemas     []model.SchemaDefinition
			sqlorder    string
			Source      string
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
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from engines where db_alias='%s'`,
				dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring, fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
				Storing.DBName, dbalias))
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
			sqlorder = fmt.Sprintf(`select TABLE_CATALOG, TABLE_SCHEMA, 
			TABLE_NAME,'TABLE_TYPE' from INFORMATION_SCHEMA.tables where TABLE_SCHEMA='%s'`, information.DBName)
			Source = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		case "mssql":
			sqlorder = fmt.Sprintf("SELECT * FROM %s.INFORMATION_SCHEMA.TABLES", information.DBName)
			Source = fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
		}

		DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		rows, err := repo.Rowmanydata(DB, sqlorder)
		defer rows.Close()
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
		for rows.Next() {
			var schema model.SchemaDefinition
			rows.Scan(&schema.TableCatalog, &schema.TableSchema, &schema.TableName, &schema.TableType)
			schemas = append(schemas, schema)
		}
		if err = rows.Err(); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		utils.SendSuccess(w, schemas)
	}
}
