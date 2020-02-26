//@title Restful API
//@version 1.0.0
//@description Define an API
//@Schemes http
//@host localhost:8080
//@BasePath /v1
package main

import (
	"net/http"
	"restful/controllers"

	"github.com/gorilla/mux"

	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

func main() {
	//create struct
	controller := controllers.Controller{}
	//create router
	router := mux.NewRouter()
	//func (r *Router) HandleFunc(path string, f func(http.ResponseWriter, *http.Request)) *Route
	//func (r *Router) Methods(methods ...string) *Route
	//connect database
	router.HandleFunc("/v1", controller.Storing()).Methods("POST")

	router.HandleFunc("/v1/_engine", controller.GetAllDB()).Methods("GET")
	router.HandleFunc("/v1/_engine", controller.ConnectDB()).Methods("POST")
	router.HandleFunc("/v1/_engine", controller.UpdateDB()).Methods("PUT")
	router.HandleFunc("/v1/_engine", controller.DeleteDB()).Methods("DELETE")

	router.HandleFunc("/v1/_schema/{db_alias}", controller.GetAllSchema()).Methods("GET")
	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}", controller.GetAllFields()).Methods("GET")
	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}", controller.CreateSchema()).Methods("POST")
	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}", controller.UpdateSchema()).Methods("PUT")
	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}", controller.DeleteSchema()).Methods("DELETE")

	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}/_field/{field_name}", controller.GetOneField()).Methods("GET")
	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}/_field/{field_name}", controller.AddOneField()).Methods("POST")
	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}/_field/{field_name}", controller.UpdateOneField()).Methods("PUT")
	router.HandleFunc("/v1/_schema/{db_alias}/{table_name}/_field/{field_name}", controller.DropOneField()).Methods("DELETE")

	router.HandleFunc("/v1/_table/{db_alias}/{table_name}", controller.GetAllData()).Methods("GET")
	router.HandleFunc("/v1/_table/{db_alias}/{table_name}", controller.AddData()).Methods("POST")
	router.HandleFunc("/v1/_table/{db_alias}/{table_name}", controller.UpdateData()).Methods("PUT")
	router.HandleFunc("/v1/_table/{db_alias}/{table_name}", controller.DeleteData()).Methods("DELETE")

	router.HandleFunc("/v1/_table/{db_alias}/{table_name}/{id}", controller.GetDataByid()).Methods("GET")
	router.HandleFunc("/v1/_table/{db_alias}/{table_name}/{id}", controller.UpdateDataByid()).Methods("PUT")
	router.HandleFunc("/v1/_table/{db_alias}/{table_name}/{id}", controller.DeleteDataByid()).Methods("DELETE")
	
	//server connect
	if err := http.ListenAndServe(":8080", router); err != nil {
		panic(err)
	}
}
