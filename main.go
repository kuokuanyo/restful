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
	router.HandleFunc("/v1", controller.GetAllDB()).Methods("GET")
	router.HandleFunc("/v1", controller.ConnectDB()).Methods("POST")
	//server connect
	if err := http.ListenAndServe(":8080", router); err != nil {
		panic(err)
	}
}
