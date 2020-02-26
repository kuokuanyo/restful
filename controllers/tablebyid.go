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
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)"
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource. example: [alias].[table]_password_[password]_by_[key1]_and_[key2]_and_..."
//@Success 200 {object} models.object "Successfully"
//@Failure 401 {object} models.Error "Unauthorized"
//@Failure 500 {object} models.Error "Internal Server Error"
//@Router /v1/_table/{db_alias}/{table_name}/{id} [delete]
func (c Controller) DeleteDataByid() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			DB             *gorm.DB
			row            *sql.Row
			rows           *sql.Rows
			information    model.Engine
			message        model.Error
			repo           repository.Repository
			params         = mux.Vars(r)
			tablename      = params["table_name"]
			dbalias        = params["db_alias"]
			id, _          = strconv.Atoi(params["id"])
			passwords      = r.URL.Query()["db_password"]
			fields         = r.URL.Query()["fields"]
			relateds       = r.URL.Query()["related"]
			password       string
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
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source)
			deletesqlorder = fmt.Sprintf(`delete from %s where id=%d`, tablename, id)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername, password, information.DBHost,
				information.DBPort, information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source)
			deletesqlorder = fmt.Sprintf(`delete from %s.dbo.%s where id=%d`, information.DBName, tablename, id)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(relateds) == 0 {
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				for i := range slicefields {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and 
						TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					case "mssql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns where 
						TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' `, information.DBName, tablename))
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

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`,
					strings.Join(slicefields, ","), tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`select %s from %s.dbo.%s where id=%d`,
					strings.Join(slicefields, ","), information.DBName, tablename, id)
			}

			var (
				value     = make([]sql.NullString, len(slicefields))
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
				data[slicefields[i]] = value[i].String
				if data[slicefields[i]] == "" {
					data[slicefields[i]] = "NULL"
				} else {
					if strings.Contains(coltype[i], "int") {
						data[slicefields[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			utils.SendSuccess(w, data)
		} else {
			related := relateds[0]
			sliceofrelated := strings.Split(related, ",")
			var fieldsbylocal, typesbylocal, otherfields []string
			var keys [][]string
			var datas []map[string]interface{}

			for i := range sliceofrelated {
				splitbyunderline := strings.Split(sliceofrelated[i], "_by_")
				keys = append(keys, strings.Split(splitbyunderline[1], "_and_"))
			}

			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				coltype = make([]string, len(slicefields))

				for i := range slicefields {
					tableandfield := strings.Split(slicefields[i], ".")
					if tableandfield[0] == tablename {
						fieldsbylocal = append(fieldsbylocal, slicefields[i])
					} else {
						otherfields = append(otherfields, slicefields[i])
					}
				}

				for i := range keys {
					fieldsbylocal = Duplicate(fieldsbylocal, keys[i], tablename)
				}

				for i := range fieldsbylocal {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from
						 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and 
						 TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					case "mssql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from 
						%s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						typesbylocal = append(typesbylocal, "varchar")
					} else {
						typesbylocal = append(typesbylocal, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from
					 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
						information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
					%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s'`, information.DBName, tablename))
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
					fieldsbylocal = append(fieldsbylocal, tablename+"."+table)
					typesbylocal = append(typesbylocal, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(fieldsbylocal) == 0 && len(typesbylocal) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, strings.Join(fieldsbylocal, ","),
					tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`select %s from %s.dbo.%s where id=%d`, strings.Join(fieldsbylocal, ","),
					information.DBName, tablename, id)
			}

			var (
				value     = make([]sql.NullString, len(fieldsbylocal))
				valuePtrs = make([]interface{}, len(fieldsbylocal))
			)
			for i := 0; i < len(fieldsbylocal); i++ {
				valuePtrs[i] = &value[i]
			}
			row = repo.RowOneData(DB, sqlorder)
			if err = row.Scan(valuePtrs...); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for i := range fieldsbylocal {
				data[fieldsbylocal[i]] = value[i].String
				if data[fieldsbylocal[i]] == "" {
					data[fieldsbylocal[i]] = "NULL"
				} else {
					if strings.Contains(typesbylocal[i], "int") {
						data[fieldsbylocal[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			datas = append(datas, data)

			for i := range sliceofrelated {
				var DBEngine *gorm.DB
				var engine model.Engine
				var order string
				var joinfield, jointype []string
				var joindatas, results []map[string]interface{}

				splitbydot := strings.Split(sliceofrelated[i], ".")
				alias := splitbydot[0]
				splitbypassword := strings.Split(splitbydot[1], "_password_")
				table := splitbypassword[0]
				splitbyunderline := strings.Split(splitbypassword[1], "_by_")
				passwordforjoin := splitbyunderline[0]
				joinkey := strings.Split(splitbyunderline[1], "_and_")

				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from engines where db_alias='%s'`, alias))
				case "mssql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
							Storing.DBName, alias))
				}
				if err = row.Scan(&engine.DBAlias, &engine.DBType, &engine.DBUsername,
					&engine.DBPassword, &engine.DBHost, &engine.DBPort,
					&engine.DBName, &engine.Maxidle, &engine.Maxopen); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if err = bcrypt.CompareHashAndPassword([]byte(engine.DBPassword), []byte(passwordforjoin)); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusUnauthorized, message)
					return
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				case "mssql":
					Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				}
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				if len(fields) > 0 {
					for m := range otherfields {
						if strings.Split(otherfields[m], ".")[0] == table {
							joinfield = append(joinfield, otherfields[m])
						}
					}
					joinfield = Duplicate(joinfield, joinkey, table)
					for m := range joinfield {
						var datatype string
						switch strings.ToLower(engine.DBType) {
						case "mysql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and
							 COLUMN_NAME='%s'`, engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						case "mssql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						}
						row.Scan(&datatype)
						if datatype == "" {
							jointype = append(jointype, "varchar")
						} else {
							jointype = append(jointype, datatype)
						}
					}
				} else {
					switch strings.ToLower(engine.DBType) {
					case "mysql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
						where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, engine.DBName, table))
					case "mssql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s'`, engine.DBName, table))
					}
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var field, datatype string
						rows.Scan(&field, &datatype)
						joinfield = append(joinfield, table+"."+field)
						jointype = append(jointype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(joinfield) == 0 && len(jointype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					order = fmt.Sprintf(`select %s from %s `, strings.Join(joinfield, ","), table)
				case "mssql":
					order = fmt.Sprintf(`select %s from %s.dbo.%s`,
						strings.Join(joinfield, ","), engine.DBName, table)
				}

				var (
					joinvalue     = make([]sql.NullString, len(joinfield))
					joinvaluePtrs = make([]interface{}, len(joinfield))
				)
				for m := 0; m < len(joinfield); m++ {
					joinvaluePtrs[m] = &joinvalue[m]
				}
				rows, err = repo.Rowmanydata(DBEngine, order)
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var joindata = make(map[string]interface{})
					rows.Scan(joinvaluePtrs...)
					for m := range joinfield {
						joindata[joinfield[m]] = joinvalue[m].String
						if joindata[joinfield[m]] == "" {
							joindata[joinfield[m]] = "NULL"
						} else {
							if strings.Contains(jointype[m], "int") {
								joindata[joinfield[m]], err = strconv.Atoi(joinvalue[m].String)
								if err != nil {
									message.Error = err.Error()
									utils.SendError(w, http.StatusInternalServerError, message)
									return
								}
							}
						}
					}
					joindatas = append(joindatas, joindata)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				for _, data := range datas {
					for _, joindata := range joindatas {
						if len(joinkey) == 1 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 2 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 3 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] &&
								data[tablename+"."+joinkey[2]] == joindata[table+"."+joinkey[2]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						}
					}
				}
				datas = results
			}

			if len(fields) > 0 {
				var finaldatas []map[string]interface{}
				for _, data := range datas {
					result := make(map[string]interface{})
					for key, value := range data {
						for _, field := range slicefields {
							if field == key {
								result[field] = value
							}
						}
					}
					finaldatas = append(finaldatas, result)
				}
				datas = finaldatas
			}
			utils.SendSuccess(w, datas)
		}

		if err = repo.Exec(DB, deletesqlorder); err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}
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
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)"
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource. example: [alias].[table]_password_[password]_by_[key1]_and_[key2]_and_..."
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
			relateds    = r.URL.Query()["related"]
			passwords   = r.URL.Query()["db_password"]
			fields      = r.URL.Query()["fields"]
			password    string
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
		splitdescription := strings.Split(description.Condition, ",")
		for i := range splitdescription {
			split := strings.Split(splitdescription[i], "=")
			split[1] = fmt.Sprintf(`'%s'`, split[1])
			splitdescription[i] = strings.Join(split, "=")
		}

		switch strings.ToLower(Storing.DBType) {
		case "mysql":
			row = repo.RowOneData(DBStoring,
				fmt.Sprintf(`select * from engines where db_alias='%s'`, dbalias))
		case "mssql":
			row = repo.RowOneData(DBStoring,
				fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`, Storing.DBName, dbalias))
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
			sqlorder = fmt.Sprintf(`update %s set %s where id=%d`, tablename, strings.Join(splitdescription, ","), id)
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb("mssql", Source)
			sqlorder = fmt.Sprintf(`update %s.dbo.%s set %s where id=%d `, information.DBName, tablename,
				strings.Join(splitdescription, ","), id)
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
		if len(relateds) == 0 {
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				for i := range slicefields {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' 
						and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					case "mssql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and 
						COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
					where TABLE_SCHEMA='%s' and TABLE_NAME='%s' `, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
					where TABLE_NAME='%s' `, information.DBName, tablename))
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

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("select %s from %s where id=%d", strings.Join(slicefields, ","), tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf("select %s from %s.dbo.%s where id=%d", strings.Join(slicefields, ","),
					information.DBName, tablename, id)
			}

			var (
				value     = make([]sql.NullString, len(slicefields))
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
				data[slicefields[i]] = value[i].String
				if data[slicefields[i]] == "" {
					data[slicefields[i]] = "NULL"
				} else {
					if strings.Contains(coltype[i], "int") {
						data[slicefields[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			utils.SendSuccess(w, data)
		} else {
			related := relateds[0]
			sliceofrelated := strings.Split(related, ",")
			var fieldsbylocal, typesbylocal, otherfields []string
			var keys [][]string
			var datas []map[string]interface{}

			for i := range sliceofrelated {
				splitbyunderline := strings.Split(sliceofrelated[i], "_by_")
				keys = append(keys, strings.Split(splitbyunderline[1], "_and_"))
			}

			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				coltype = make([]string, len(slicefields))

				for i := range slicefields {
					tableandfield := strings.Split(slicefields[i], ".")
					if tableandfield[0] == tablename {
						fieldsbylocal = append(fieldsbylocal, slicefields[i])
					} else {
						otherfields = append(otherfields, slicefields[i])
					}
				}

				for i := range keys {
					fieldsbylocal = Duplicate(fieldsbylocal, keys[i], tablename)
				}

				for i := range fieldsbylocal {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from
						 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and 
						 TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					case "mssql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from 
						%s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						typesbylocal = append(typesbylocal, "varchar")
					} else {
						typesbylocal = append(typesbylocal, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from
					 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
						information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
					%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s'`, information.DBName, tablename))
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
					fieldsbylocal = append(fieldsbylocal, tablename+"."+table)
					typesbylocal = append(typesbylocal, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(fieldsbylocal) == 0 && len(typesbylocal) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, strings.Join(fieldsbylocal, ","),
					tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`select %s from %s.dbo.%s where id=%d`, strings.Join(fieldsbylocal, ","),
					information.DBName, tablename, id)
			}

			var (
				value     = make([]sql.NullString, len(fieldsbylocal))
				valuePtrs = make([]interface{}, len(fieldsbylocal))
			)
			for i := 0; i < len(fieldsbylocal); i++ {
				valuePtrs[i] = &value[i]
			}
			row = repo.RowOneData(DB, sqlorder)
			if err = row.Scan(valuePtrs...); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for i := range fieldsbylocal {
				data[fieldsbylocal[i]] = value[i].String
				if data[fieldsbylocal[i]] == "" {
					data[fieldsbylocal[i]] = "NULL"
				} else {
					if strings.Contains(typesbylocal[i], "int") {
						data[fieldsbylocal[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			datas = append(datas, data)

			for i := range sliceofrelated {
				var DBEngine *gorm.DB
				var engine model.Engine
				var order string
				var joinfield, jointype []string
				var joindatas, results []map[string]interface{}

				splitbydot := strings.Split(sliceofrelated[i], ".")
				alias := splitbydot[0]
				splitbypassword := strings.Split(splitbydot[1], "_password_")
				table := splitbypassword[0]
				splitbyunderline := strings.Split(splitbypassword[1], "_by_")
				passwordforjoin := splitbyunderline[0]
				joinkey := strings.Split(splitbyunderline[1], "_and_")

				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from engines where db_alias='%s'`, alias))
				case "mssql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
							Storing.DBName, alias))
				}
				if err = row.Scan(&engine.DBAlias, &engine.DBType, &engine.DBUsername,
					&engine.DBPassword, &engine.DBHost, &engine.DBPort,
					&engine.DBName, &engine.Maxidle, &engine.Maxopen); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if err = bcrypt.CompareHashAndPassword([]byte(engine.DBPassword), []byte(passwordforjoin)); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusUnauthorized, message)
					return
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				case "mssql":
					Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				}
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				if len(fields) > 0 {
					for m := range otherfields {
						if strings.Split(otherfields[m], ".")[0] == table {
							joinfield = append(joinfield, otherfields[m])
						}
					}
					joinfield = Duplicate(joinfield, joinkey, table)
					for m := range joinfield {
						var datatype string
						switch strings.ToLower(engine.DBType) {
						case "mysql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and
							 COLUMN_NAME='%s'`, engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						case "mssql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						}
						row.Scan(&datatype)
						if datatype == "" {
							jointype = append(jointype, "varchar")
						} else {
							jointype = append(jointype, datatype)
						}
					}
				} else {
					switch strings.ToLower(engine.DBType) {
					case "mysql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
						where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, engine.DBName, table))
					case "mssql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s'`, engine.DBName, table))
					}
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var field, datatype string
						rows.Scan(&field, &datatype)
						joinfield = append(joinfield, table+"."+field)
						jointype = append(jointype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(joinfield) == 0 && len(jointype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					order = fmt.Sprintf(`select %s from %s `, strings.Join(joinfield, ","), table)
				case "mssql":
					order = fmt.Sprintf(`select %s from %s.dbo.%s`,
						strings.Join(joinfield, ","), engine.DBName, table)
				}

				var (
					joinvalue     = make([]sql.NullString, len(joinfield))
					joinvaluePtrs = make([]interface{}, len(joinfield))
				)
				for m := 0; m < len(joinfield); m++ {
					joinvaluePtrs[m] = &joinvalue[m]
				}
				rows, err = repo.Rowmanydata(DBEngine, order)
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var joindata = make(map[string]interface{})
					rows.Scan(joinvaluePtrs...)
					for m := range joinfield {
						joindata[joinfield[m]] = joinvalue[m].String
						if joindata[joinfield[m]] == "" {
							joindata[joinfield[m]] = "NULL"
						} else {
							if strings.Contains(jointype[m], "int") {
								joindata[joinfield[m]], err = strconv.Atoi(joinvalue[m].String)
								if err != nil {
									message.Error = err.Error()
									utils.SendError(w, http.StatusInternalServerError, message)
									return
								}
							}
						}
					}
					joindatas = append(joindatas, joindata)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				for _, data := range datas {
					for _, joindata := range joindatas {
						if len(joinkey) == 1 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 2 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 3 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] &&
								data[tablename+"."+joinkey[2]] == joindata[table+"."+joinkey[2]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						}
					}
				}
				datas = results
			}

			if len(fields) > 0 {
				var finaldatas []map[string]interface{}
				for _, data := range datas {
					result := make(map[string]interface{})
					for key, value := range data {
						for _, field := range slicefields {
							if field == key {
								result[field] = value
							}
						}
					}
					finaldatas = append(finaldatas, result)
				}
				datas = finaldatas
			}
			utils.SendSuccess(w, datas)
		}
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
//@Param fields query array false "Comma-delimited list of properties to be returned for each resource, "*" returns all properties. If using related parameters, please clearly indicate the table name and field name (for example: table.fielaname)"
//@Param related query array false "Comma-delimited list of related names to retrieve for each resource. example: [alias].[table]_password_[password]_by_[key1]_and_[key2]_and_..."
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
			passwords   = r.URL.Query()["db_password"]
			fields      = r.URL.Query()["fields"]
			relateds    = r.URL.Query()["related"]
			password    string
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
			DB, err = repo.ConnectDb(information.DBType, Source) //connect db
		case "mssql":
			Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
				information.DBUsername,
				password,
				information.DBHost,
				information.DBPort,
				information.DBName)
			DB, err = repo.ConnectDb(information.DBType, Source)
		}
		if err != nil {
			message.Error = err.Error()
			utils.SendError(w, http.StatusInternalServerError, message)
			return
		}

		if len(relateds) == 0 {
			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				for i := range slicefields {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' 
							and TABLE_NAME='%s' and COLUMN_NAME='%s' `, information.DBName, tablename, slicefields[i]))
					case "mssql":
						row = repo.RowOneData(DB,
							fmt.Sprintf(`select Data_Type from %s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' 
							and COLUMN_NAME='%s'`, information.DBName, tablename, slicefields[i]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						coltype = append(coltype, "varchar")
					} else {
						coltype = append(coltype, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns
						 where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB,
						fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s'`, information.DBName, tablename))
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
				if len(slicefields) == 0 && len(coltype) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf("select %s from %s where id=%d",
					strings.Join(slicefields, ","), tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf("select %s from %s.dbo.%s where id=%d",
					strings.Join(slicefields, ","), information.DBName, tablename, id)
			}

			var (
				value     = make([]sql.NullString, len(slicefields))
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
				data[slicefields[i]] = value[i].String
				if data[slicefields[i]] == "" {
					data[slicefields[i]] = "NULL"
				} else {
					if strings.Contains(coltype[i], "int") {
						data[slicefields[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			utils.SendSuccess(w, data)
		} else {
			related := relateds[0]
			sliceofrelated := strings.Split(related, ",")
			var fieldsbylocal, typesbylocal, otherfields []string
			var keys [][]string
			var datas []map[string]interface{}

			for i := range sliceofrelated {
				splitbyunderline := strings.Split(sliceofrelated[i], "_by_")
				keys = append(keys, strings.Split(splitbyunderline[1], "_and_"))
			}

			if len(fields) > 0 {
				slicefields = strings.Split(fields[0], ",")
				coltype = make([]string, len(slicefields))

				for i := range slicefields {
					tableandfield := strings.Split(slicefields[i], ".")
					if tableandfield[0] == tablename {
						fieldsbylocal = append(fieldsbylocal, slicefields[i])
					} else {
						otherfields = append(otherfields, slicefields[i])
					}
				}

				for i := range keys {
					fieldsbylocal = Duplicate(fieldsbylocal, keys[i], tablename)
				}

				for i := range fieldsbylocal {
					var datatype string
					switch strings.ToLower(information.DBType) {
					case "mysql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from
						 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and 
						 TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					case "mssql":
						row = repo.RowOneData(DB, fmt.Sprintf(`select Data_Type from 
						%s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
							information.DBName, tablename, strings.Split(fieldsbylocal[i], ".")[1]))
					}
					row.Scan(&datatype)
					if datatype == "" {
						typesbylocal = append(typesbylocal, "varchar")
					} else {
						typesbylocal = append(typesbylocal, datatype)
					}
				}
			} else {
				switch strings.ToLower(information.DBType) {
				case "mysql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from
					 INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`,
						information.DBName, tablename))
				case "mssql":
					rows, err = repo.Rowmanydata(DB, fmt.Sprintf(`select COLUMN_NAME, Data_Type from 
					%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s'`, information.DBName, tablename))
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
					fieldsbylocal = append(fieldsbylocal, tablename+"."+table)
					typesbylocal = append(typesbylocal, datatype)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if len(fieldsbylocal) == 0 && len(typesbylocal) == 0 {
					message.Error = "The table does not exist."
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
			}

			switch strings.ToLower(information.DBType) {
			case "mysql":
				sqlorder = fmt.Sprintf(`select %s from %s where id=%d`, strings.Join(fieldsbylocal, ","),
					tablename, id)
			case "mssql":
				sqlorder = fmt.Sprintf(`select %s from %s.dbo.%s where id=%d`, strings.Join(fieldsbylocal, ","),
					information.DBName, tablename, id)
			}

			var (
				value     = make([]sql.NullString, len(fieldsbylocal))
				valuePtrs = make([]interface{}, len(fieldsbylocal))
			)
			for i := 0; i < len(fieldsbylocal); i++ {
				valuePtrs[i] = &value[i]
			}
			row = repo.RowOneData(DB, sqlorder)
			if err = row.Scan(valuePtrs...); err != nil {
				message.Error = err.Error()
				utils.SendError(w, http.StatusInternalServerError, message)
				return
			}
			for i := range fieldsbylocal {
				data[fieldsbylocal[i]] = value[i].String
				if data[fieldsbylocal[i]] == "" {
					data[fieldsbylocal[i]] = "NULL"
				} else {
					if strings.Contains(typesbylocal[i], "int") {
						data[fieldsbylocal[i]], err = strconv.Atoi(value[i].String)
						if err != nil {
							message.Error = err.Error()
							utils.SendError(w, http.StatusInternalServerError, message)
							return
						}
					}
				}
			}
			datas = append(datas, data)

			for i := range sliceofrelated {
				var DBEngine *gorm.DB
				var engine model.Engine
				var order string
				var joinfield, jointype []string
				var joindatas, results []map[string]interface{}

				splitbydot := strings.Split(sliceofrelated[i], ".")
				alias := splitbydot[0]
				splitbypassword := strings.Split(splitbydot[1], "_password_")
				table := splitbypassword[0]
				splitbyunderline := strings.Split(splitbypassword[1], "_by_")
				passwordforjoin := splitbyunderline[0]
				joinkey := strings.Split(splitbyunderline[1], "_and_")

				switch strings.ToLower(Storing.DBType) {
				case "mysql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from engines where db_alias='%s'`, alias))
				case "mssql":
					row = repo.RowOneData(DBStoring,
						fmt.Sprintf(`select * from %s.dbo.engines where db_alias='%s'`,
							Storing.DBName, alias))
				}
				if err = row.Scan(&engine.DBAlias, &engine.DBType, &engine.DBUsername,
					&engine.DBPassword, &engine.DBHost, &engine.DBPort,
					&engine.DBName, &engine.Maxidle, &engine.Maxopen); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				if err = bcrypt.CompareHashAndPassword([]byte(engine.DBPassword), []byte(passwordforjoin)); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusUnauthorized, message)
					return
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					Source := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				case "mssql":
					Source := fmt.Sprintf("sqlserver://%s:%s@%s:%s? database=%s",
						engine.DBUsername, passwordforjoin,
						engine.DBHost, engine.DBPort, engine.DBName)
					DBEngine, err = repo.ConnectDb(engine.DBType, Source)
				}
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				if len(fields) > 0 {
					for m := range otherfields {
						if strings.Split(otherfields[m], ".")[0] == table {
							joinfield = append(joinfield, otherfields[m])
						}
					}
					joinfield = Duplicate(joinfield, joinkey, table)
					for m := range joinfield {
						var datatype string
						switch strings.ToLower(engine.DBType) {
						case "mysql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							INFORMATION_SCHEMA.columns where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and
							 COLUMN_NAME='%s'`, engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						case "mssql":
							row = repo.RowOneData(DBEngine, fmt.Sprintf(`select Data_Type from 
							%s.INFORMATION_SCHEMA.columns where TABLE_NAME='%s' and COLUMN_NAME='%s'`,
								engine.DBName, table, strings.Split(joinfield[m], ".")[1]))
						}
						row.Scan(&datatype)
						if datatype == "" {
							jointype = append(jointype, "varchar")
						} else {
							jointype = append(jointype, datatype)
						}
					}
				} else {
					switch strings.ToLower(engine.DBType) {
					case "mysql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from INFORMATION_SCHEMA.columns 
						where TABLE_SCHEMA='%s' and TABLE_NAME='%s'`, engine.DBName, table))
					case "mssql":
						rows, err = repo.Rowmanydata(DBEngine,
							fmt.Sprintf(`select COLUMN_NAME, Data_Type from %s.INFORMATION_SCHEMA.columns 
						where TABLE_NAME='%s'`, engine.DBName, table))
					}
					defer rows.Close()
					if err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					for rows.Next() {
						var field, datatype string
						rows.Scan(&field, &datatype)
						joinfield = append(joinfield, table+"."+field)
						jointype = append(jointype, datatype)
					}
					if err = rows.Err(); err != nil {
						message.Error = err.Error()
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
					if len(joinfield) == 0 && len(jointype) == 0 {
						message.Error = "The table does not exist."
						utils.SendError(w, http.StatusInternalServerError, message)
						return
					}
				}

				switch strings.ToLower(engine.DBType) {
				case "mysql":
					order = fmt.Sprintf(`select %s from %s `, strings.Join(joinfield, ","), table)
				case "mssql":
					order = fmt.Sprintf(`select %s from %s.dbo.%s`,
						strings.Join(joinfield, ","), engine.DBName, table)
				}

				var (
					joinvalue     = make([]sql.NullString, len(joinfield))
					joinvaluePtrs = make([]interface{}, len(joinfield))
				)
				for m := 0; m < len(joinfield); m++ {
					joinvaluePtrs[m] = &joinvalue[m]
				}
				rows, err = repo.Rowmanydata(DBEngine, order)
				defer rows.Close()
				if err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}
				for rows.Next() {
					var joindata = make(map[string]interface{})
					rows.Scan(joinvaluePtrs...)
					for m := range joinfield {
						joindata[joinfield[m]] = joinvalue[m].String
						if joindata[joinfield[m]] == "" {
							joindata[joinfield[m]] = "NULL"
						} else {
							if strings.Contains(jointype[m], "int") {
								joindata[joinfield[m]], err = strconv.Atoi(joinvalue[m].String)
								if err != nil {
									message.Error = err.Error()
									utils.SendError(w, http.StatusInternalServerError, message)
									return
								}
							}
						}
					}
					joindatas = append(joindatas, joindata)
				}
				if err = rows.Err(); err != nil {
					message.Error = err.Error()
					utils.SendError(w, http.StatusInternalServerError, message)
					return
				}

				for _, data := range datas {
					for _, joindata := range joindatas {
						if len(joinkey) == 1 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 2 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						} else if len(joinkey) == 3 {
							if data[tablename+"."+joinkey[0]] == joindata[table+"."+joinkey[0]] &&
								data[tablename+"."+joinkey[1]] == joindata[table+"."+joinkey[1]] &&
								data[tablename+"."+joinkey[2]] == joindata[table+"."+joinkey[2]] {
								result := make(map[string]interface{})
								for datakey, datavalue := range data {
									for key, value := range joindata {
										result[datakey] = datavalue
										result[key] = value
									}
								}
								results = append(results, result)
							}
						}
					}
				}
				datas = results
			}

			if len(fields) > 0 {
				var finaldatas []map[string]interface{}
				for _, data := range datas {
					result := make(map[string]interface{})
					for key, value := range data {
						for _, field := range slicefields {
							if field == key {
								result[field] = value
							}
						}
					}
					finaldatas = append(finaldatas, result)
				}
				datas = finaldatas
			}
			utils.SendSuccess(w, datas)
		}
	}
}
