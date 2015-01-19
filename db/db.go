package db

import (
	"github.com/gocql/gocql"
	"strconv"
	"errors"
	"time"
	"fmt"
	"strings"
	"math/rand"
	"log"
)

var (
	Servers = []string{"10.1.51.65","10.1.51.66"}
	Keyspace string = "counterks"
	KeyspaceBlock string = "cobrand"
	Types = map[string]string {"1":"searchform","2":"informers"}
)

const TimeShortForm = "2006-10-02"

type Row struct {
	Client_id string
	Client_type int
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

	//cluster := gocql.NewCluster("10.1.18.122")
	cluster := gocql.NewCluster("10.1.51.65","10.1.51.66")
	cluster.Keyspace = Keyspace
	//cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	if (err != nil) {
		return err
	}

	defer session.Close()
	
	//clientId, err := strconv.ParseInt(params["client_id"], 10, 64)
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
		params["client_id"], clientType, timestamp).Exec(); err != nil {
		return err
	}

	return err
}

func Get(params map[string]string) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)
	var (
		err error
		neededParams = []string{"client_id", "client_type", "from", "to"}
	)
	cluster := gocql.NewCluster("10.1.51.65","10.1.51.66")
	cluster.Keyspace = Keyspace
	cluster.Consistency = gocql.One
	session, _ := cluster.CreateSession()
	for _, param := range neededParams {
		if(params[param] == "") {
			err := errors.New(fmt.Sprintf("param '%s' required", param))
			return result, err
		}
	}


	fromT := fmt.Sprintf("%sT00:00:00Z", params["from"])
	toT := fmt.Sprintf("%sT00:00:00Z", params["to"])

	location, _ := time.LoadLocation("Europe/Kiev")
	from, err := time.ParseInLocation(time.RFC3339, fromT, location)
	to, err := time.ParseInLocation(time.RFC3339, toT, location)

	if (err != nil) {
		return result, err
	}

	//clientId, err := strconv.ParseInt(params["client_id"], 10, 64)
	clientType, err := strconv.ParseInt(params["client_type"], 10, 64)
	if (err != nil) {
		return result, err
	}

	const timeFormat = "2012-01-03 00:00:00 +0200 UTC"
	fromMas := strings.Split(fmt.Sprintf("%s", from), " UTC")
	toMas := strings.Split(fmt.Sprintf("%s", to), " UTC")

	query := fmt.Sprintf("SELECT client_id, client_type, time, count FROM cobrand_count WHERE client_id='%s' AND client_type=%d AND time >= '%s' AND time <= '%s'", params["client_id"], clientType, fromMas[0], toMas[0])

	iter := session.Query(query).Iter()

	//output := make(chan map[int]Row)
	//row := Row{}
	var cid string
	var ccount,ctype int
	var ctime time.Time;


	// create concurrent queries
	//var counter int = 0

	//go func(){
	for iter.Scan(&cid, &ctype, &ctime, &ccount) {
		//time := timedata;
		row := make(map[string]string)
		day := ctime.Format(time.RFC3339)
		dayTime := strings.Split(day, "T")
		//res := Row{cid, ctype, fmt.Sprintf("%s", dayTime[0]), ccount}

		row["client_id"] = cid
		row["client_type"] = fmt.Sprintf("%d", ctype)
		row["client_time"] = fmt.Sprintf("%s", dayTime[0])
		row["client_count"] = fmt.Sprintf("%d", ccount)


		result[row["client_time"]] = row

	}

		//output <- response
	//}();
	if err := iter.Close(); err != nil {
		return result, err
	}

	defer session.Close()
	return result, err
}

func Blocks(key string, blocktype string) (code string, err error){
	var count int

	if (key == "") {
		err = errors.New("param: key is empty!")
		return
	}
	if (blocktype == "") {
		err = errors.New("param: blocktype is empty!")
		return
	}

	//cluster := gocql.NewCluster("10.1.18.122")
	cluster := gocql.NewCluster("10.1.51.65","10.1.51.66")

	cluster.NumConns = 1
	cluster.NumStreams = 64
	cluster.MaxPreparedStmts = 2000
	cluster.Keyspace = KeyspaceBlock
	sessionGet, _ := cluster.CreateSession()
	defer sessionGet.Close()


	table := Types[blocktype]
	//number of items
	if err = sessionGet.Query(fmt.Sprintf("SELECT count FROM %s WHERE key = '%s'",
		table, fmt.Sprintf("%s1",key))).Consistency(gocql.One).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 1}).Scan(&count); err != nil {
		return
	}

	if (count == 0) {
		count = 20
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randNum := r.Intn(count)
	if (randNum == 0) {
		randNum = 1
	}
	newKey := fmt.Sprintf("%s%d", key, randNum)
	//log.Print(newKey)


	if err = sessionGet.Query(fmt.Sprintf("SELECT code FROM %s WHERE key = '%s'",
		table, newKey)).Consistency(gocql.One).RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 1}).Scan(&code); err != nil {
		log.Print(err)

		return
	}


	// SET COUNTER
	if(code != "") {
		go func() {
			row := make(map[string]string)
			row["client_type"] = blocktype
			row["client_id"] = key
			Put(row)
		}()
	}
	// END

	return
}
