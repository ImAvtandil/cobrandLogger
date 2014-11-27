package main

import (
	"github.com/polluxx/cobrandLogger/db"
	"net/http"
	"time"
	"regexp"
	"log"
	"encoding/json"
)

type Resp struct {
	Code int
	Message string
	Data map[string]string
}

func main() {
	http.HandleFunc("/put", mainHandler(putHandler));
	http.HandleFunc("/get", mainHandler(getHandler));

	s := &http.Server{
		Addr:           ":8082",
		//Handler:        Handle,
		ReadTimeout:    120 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Fatal(s.ListenAndServe())
}

func mainHandler (fn func(http.ResponseWriter, *http.Request, map[string]string)) http.HandlerFunc {
	var validPath = regexp.MustCompile("^/(put|get)")
	return func(w http.ResponseWriter, r *http.Request) {
		mess := validPath.FindStringSubmatch(r.URL.Path)
		if mess == nil {
			http.NotFound(w,r);
			return
		}

		r.ParseForm();
		queryParams := make(map[string]string)
		for index, value := range r.Form {
			queryParams[index] = value[0];
		}

		fn(w, r, queryParams)
	}
}

func putHandler (w http.ResponseWriter, r *http.Request, params map[string]string) {
	response := Resp{}
	response.Code = 200;
	response.Message = "OKe";

	err := db.Put(params)
	if (err != nil) {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
	}
	makeResp(w ,r ,response)
}


func getHandler (w http.ResponseWriter, r *http.Request, params map[string]string) {
	response := Resp{}
	response.Code = 200;
	response.Message = "OK";

	data, err := db.Get(params)
	if (err != nil) {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
	}
	response.Data = data;
	makeResp(w ,r ,response)
}

func makeResp(w http.ResponseWriter, r *http.Request, data Resp) {
	jsn, err := json.Marshal(data)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Headers", "authorization")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Write(jsn);
}
