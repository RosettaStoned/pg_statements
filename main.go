package main

import (
    "fmt"
    "log"
    "time"
    "io/ioutil"
    "net/http"
    "github.com/gorilla/websocket"
    "database/sql"
    _ "github.com/lib/pq"
    "encoding/json"
    "crypto/md5"
    "encoding/hex"
)

const (
    host     = "/var/run/postgresql"
    port     = 5432
    user     = "kod"
    dbname   = "kod"
)

type Stm struct {
    Running bool `json:"running"`
    Event string `json:"event"`
    Database string `json:"database"`
    BackendPid int `json:"backend_pid"`
    BackendState string `json:"backend_state"`
    BackendWaiting *string `json:"backend_waiting"`
    QueryStart time.Time `json:"query_start"`
    Query string `json:"query"`
    LockerBackendPid *int `json:"locker_backend_pid"`
    LockerQueryStart *string `json:"locker_query_start"`
    LockerQuery *string `json:"locker_query"`
    LockType *string `json:"lock_type"`
    LockMode *string `json:"lock_mode"`
    LockGranted *bool `json:"lock_granted"`
}

var lm map[chan *Stm] struct{}
var stms map[string] *Stm

func getMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
    content, err := ioutil.ReadFile("index.html")
    if err != nil {
        log.Println("Could not open file.", err)
    }
    fmt.Fprintf(w, "%s", content)
}

func wsHandler (w http.ResponseWriter, r *http.Request) {

    conn, err := websocket.Upgrade(w, r, w.Header(), 1024, 1024)
    if err != nil {
        http.Error(w, "Could not open websocket connection", http.StatusBadRequest)
    }

    lChan := make(chan *Stm)

    if lm == nil {
        lm = make(map[chan *Stm]struct{})
    }

    lm[lChan] = struct{}{}

    for {
        select {
        case stm := <-lChan:

            log.Println(stm.BackendPid);
            log.Println(stm.Event);


            stmJson, err := json.Marshal(stm)
            if err != nil {
                log.Fatal(err)
            }


            if err = conn.WriteJSON(string(stmJson)); err != nil {
                log.Println(err)

                delete(lm, lChan)
            }

        }
    }
}

func main() {

    psqlInfo := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
    host, port, user, dbname)
    db, err := sql.Open("postgres", psqlInfo)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    err = db.Ping()
    if err != nil {
        panic(err)
    }

    log.Println("Successfully connected!")


    go func() {


        stms = make(map[string] *Stm)
        tickChan := time.NewTicker(time.Millisecond * 300).C

        for {
            select {
            case <- tickChan:
                log.Println("Ticker ticked")

                go func() {

                    for _, stm := range stms {
                        stm.Running = false;
                    }

                    rows, err := db.Query(`
                    SELECT * 
                    FROM pg_statements PS;
                    `)

                    if err != nil {
                        log.Fatal(err)
                    }
                    defer rows.Close()

                    for rows.Next() {
                        stm := Stm{}

                        err := rows.Scan(&stm.Database,
                        &stm.BackendPid,
                        &stm.BackendState,
                        &stm.BackendWaiting,
                        &stm.QueryStart,
                        &stm.Query,
                        &stm.LockerBackendPid,
                        &stm.LockerQueryStart,
                        &stm.LockerQuery,
                        &stm.LockType,
                        &stm.LockMode,
                        &stm.LockGranted)

                        if err != nil {
                            log.Fatal(err)
                        }


                        stm.Running = true;

                        bp := string(stm.BackendPid);
                        q := stm.Query;
                        qs := stm.QueryStart.String(); 

                        key := getMD5Hash(bp + q + qs); 

                        if _, ok := stms[key]; ok {
                            stm.Event = "UPDATE"
                        } else {
                            stm.Event = "INSERT"
                        }

                        stms[key] =  &stm;


                    }

                    log.Println(len(stms))

                    for key, stm := range stms {

                        if stm.Running == false {
                            stm.Event = "DELETE"
                            delete(stms, key)
                        }


                        for c, _ := range(lm) {
                            c <- stm
                        }

                    }



                }()
            }

        }

    }();


    http.HandleFunc("/ws", wsHandler)
    http.HandleFunc("/", rootHandler)


    panic(http.ListenAndServe(":8080", nil))
}

