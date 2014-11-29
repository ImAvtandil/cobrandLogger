package main

import (
	"github.com/polluxx/cobrandLogger/db"
	//"github.com/gorilla/schema"
	"net/http"
	"time"
	"regexp"
	"log"
	"encoding/json"
	"fmt"
	"io/ioutil"
	//"reflect"
	//"strings"
)

type Resp struct {
	Code int
	Message string
	Data map[string]map[string]string
}

type Row struct {
	Client_id string
	Client_type int
	Time string
	Count int
}

const CoobUrl = "http://cobrand.ria.com"

func main() {
	http.HandleFunc("/put", mainHandler(putHandler));
	http.HandleFunc("/get", mainHandler(getHandler));
	http.HandleFunc("/getter", mainHandler(getterHandler));

	s := &http.Server{
		Addr:           ":8082",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Fatal(s.ListenAndServe())
}

func mainHandler (fn func(http.ResponseWriter, *http.Request, map[string]string)) http.HandlerFunc {
	var validPath = regexp.MustCompile("^/(put|get|getter)")
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
	response.Message = "OK";

	go func() {
		err := db.Put(params)
		if (err != nil) {
			response.Code = http.StatusInternalServerError
			response.Message = err.Error()
		}
	}()

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

	/*result := make(map[string]map[string]string)

	decoder := schema.NewDecoder()
	err = decoder.Decode(data, result)
	//Reparse(data)
	fmt.Printf("%v", result)*/
	response.Data = data;
	makeResp(w ,r ,response)
}


func getterHandler (w http.ResponseWriter, r *http.Request, params map[string]string) {
	response := Resp{}
	response.Code = 200;
	response.Message = "OK";

	// PUT VISIT DATA
	go func() {
		var client_type string = "2";
		if (params["client_type"] != "") {
			client_type = params["client_type"];
		}
		visitParams := map[string]string{"client_id":params["key"], "client_type":client_type}
		err := db.Put(visitParams)
		if (err != nil) {
			log.Print(err)
		}
	}()
	// END

	resp, err := http.Get(fmt.Sprintf("%s/service/get/findinformer?key=%s", CoobUrl, params["key"]))
	if (err != nil) {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Headers", "authorization")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Write(body);
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
