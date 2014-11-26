package db

import (
	"github.com/gocql/gocql"
	"strconv"
	"errors"
	"time"
	"fmt"
	"strings"
)

var (
	Servers = []string{"10.1.51.65","10.1.51.66"}
	Keyspace string = "counterks"
)

const TimeShortForm = "2006-Nov-02"

type Row struct {
	Client_id string
	Client_type string
	Time string
	Count int
}

func Put(params map[string]string) error{
	var err error

	if (params["client_id"] == "") {
		return errors.New("param: clientId is empty!")
	}
	if (params["client_type"] == "") {
		return errors.New("param: clientType is empty!")
	}

	cluster := gocql.NewCluster("10.1.51.65","10.1.51.66")
	cluster.Keyspace = Keyspace
	cluster.Consistency = gocql.One
	session, _ := cluster.CreateSession()

	defer session.Close()
	
	clientId, err := strconv.ParseInt(params["client_id"], 10, 64)
	clientType, err := strconv.ParseInt(params["client_type"], 10, 64)
	if (err != nil) {
		return err
	}

	location, _ := time.LoadLocation("Europe/Kiev")
	currentTime := time.Now()

	timestamp := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0,0,0,0, location)

	// insert data
	if err := session.Query(`UPDATE cobrand_count
							SET count = count + 1
 							WHERE client_id=?
 							AND client_type=?
 							AND time=?`,
		clientId, clientType, timestamp).Exec(); err != nil {
		return err
	}

	return err
}

func Get(params map[string]string) (map[string]string, error) {
	var (
		result map[string]string
		err error
		neededParams = []string{"client_id", "client_type", "from", "to"}
	)
	cluster := gocql.NewCluster("10.1.18.122")
	cluster.Keyspace = Keyspace
	//cluster.Consistency = gocql.One
	session, _ := cluster.CreateSession()
	for _, param := range neededParams {
		if(params[param] == "") {
			err := errors.New(fmt.Sprintf("param '%s' required", param))
			return result, err
		}
	}

	//fmt.Printf("%v", params)


	location, _ := time.LoadLocation("Europe/Kiev")
	from, err := time.ParseInLocation(TimeShortForm, fmt.Sprintf("%s", params["from"]), location)
	to, err := time.ParseInLocation(TimeShortForm, fmt.Sprintf("%s", params["to"]), location)
	//fmt.Printf("%v - %v", from, to)
	if (err != nil) {
		return result, err
	}

	clientId, err := strconv.ParseInt(params["client_id"], 10, 64)
	clientType, err := strconv.ParseInt(params["client_type"], 10, 64)
	if (err != nil) {
		return result, err
	}

	const timeFormat = "2012-01-03 00:00:00 +0200"
	fromMas := strings.Split(fmt.Sprintf("%s", from), " EET")
	toMas := strings.Split(fmt.Sprintf("%s", to), " EET")
	//fmt.Printf("%v - %v", from, to)
	query := fmt.Sprintf("SELECT client_id, client_type, time, count FROM cobrand_count WHERE client_id=%d	AND client_type=%d 	AND time >= '%s' AND time <= '%s'", clientId, clientType, fromMas[0], toMas[0])
	fmt.Printf("%s", query)
	iter := session.Query(query).Iter()

	//output := make(chan map[int]Row)
	//row := Row{}
	var cId,cType,cTime string
	var cCount int


	response := make(map[int]Row)
	// create concurrent queries
	var counter int = 0
	//go func(){
		for iter.Scan(&cId, &cType, &cTime, &cCount) {

			//time := timedata;
			//day := time.In(location).Format(dayform)
			//dayTime := strings.Split(day, "Z")
			res := Row{cId, cType, cTime, cCount}
			fmt.Printf("%v", res)
			response[counter] = res
			counter++
		}

		//output <- response
	//}();

	fmt.Printf("%v", response)

	if err := iter.Close(); err != nil {
		//log.Fatal(err)
	}

	defer session.Close()
	return result, err
}
