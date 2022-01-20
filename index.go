package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/godror/godror"
)

func sendUser(userId int, ch chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	_, err := http.Get("https://gorest.co.in/public/v1/posts/" + strconv.Itoa(userId))
	if err != nil {
		log.Println("err handle it")
	}
	// defer resp.Body.Close()
	// b, err := ioutil.ReadAll(resp.Body)
	// if err != nil {
	// 	log.Println("err handle it")
	// }
	ch <- userId
}

func process() {
	ch := make(chan int)

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go sendUser(i, ch, &wg)
	}
	// close the channel in the background
	go func() {
		wg.Wait()
		close(ch)
	}()
	// read from channel as they come in until its closed

	for res := range ch {
		fmt.Println(res)
	}
}

type responseJson struct {
	Success bool   `json:"status"`
	Message string `json:"message"`
	Data    string `json:"data"`
}

func main() {

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/synchronize", func(w http.ResponseWriter, r *http.Request) {

		db, _ := sql.Open("godror", `user="jne" password="JNEmerdeka123!" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=34.101.218.194)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbdev)))"`)

		stmt, err := db.Prepare("SELECT * FROM TICKET_CATEGORY")
		if err != nil {
			log.Fatal(err)
		}

		res, err := stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(res)

		// process()

		resp := &responseJson{}
		resp.Data = "Hallo"
		resp.Message = "Hallo"
		resp.Success = true

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	fmt.Println("Server started in 8085")

	err := http.ListenAndServe(":8085", r)

	if err != nil {
		log.Fatal(err)
	}

}
