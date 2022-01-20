package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/godror/godror"
)

type responseJson struct {
	Success bool                   `json:"status"`
	Message string                 `json:"message"`
	Data    [][]*TicketCategoryDao `json:"data"`
}

type TicketCategoryDao struct {
	ID          string
	DESCRIPTION string
}

func callApi(userId int, ch chan<- []*TicketCategoryDao, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	ticketCategoryList := make([]*TicketCategoryDao, 0)

	q, err := db.Query("SELECT CATEGORY_ID, CATEGORY_DESCRIPTION FROM TICKET_CATEGORY")
	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		ticketCategory := new(TicketCategoryDao)
		if err := q.Scan(&ticketCategory.ID, &ticketCategory.DESCRIPTION); err != nil {
			log.Fatal(err)
		}

		ticketCategoryList = append(ticketCategoryList, ticketCategory)

	}

	fmt.Println(userId)

	ch <- ticketCategoryList
}

func main() {

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/tmp", func(w http.ResponseWriter, r *http.Request) {

		// ticketCategoryList := make([]*TicketCategoryDao, 0)

		db, _ := sql.Open("godror", `user="jne" password="JNEmerdeka123!" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=34.101.218.194)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbdev)))"`)

		q, err := db.Query("SELECT CATEGORY_ID, CATEGORY_DESCRIPTION FROM TICKET_CATEGORY")
		if err != nil {
			log.Fatal(err)
		}

		for q.Next() {
			ticketCategory := new(TicketCategoryDao)
			if err := q.Scan(&ticketCategory.ID, &ticketCategory.DESCRIPTION); err != nil {
				log.Fatal(err)
			}

			// ticketCategoryList = append(ticketCategoryList, ticketCategory)

		}

		// process()

		resp := &responseJson{}
		// resp.Data = ticketCategoryList
		resp.Message = "Hallo"
		resp.Success = true

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	r.Get("/synchronize", func(w http.ResponseWriter, r *http.Request) {

		db, _ := sql.Open("godror", `user="jne" password="JNEmerdeka123!" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=34.101.218.194)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbdev)))"`)

		ch := make(chan []*TicketCategoryDao)

		var wg sync.WaitGroup

		for i := 0; i < 30; i++ {
			wg.Add(1)
			go callApi(i, ch, &wg, db)
		}

		// close the channel in the background
		go func() {
			wg.Wait()
			close(ch)
		}()

		// read from channel as they come in until its closed

		resData := make([][]*TicketCategoryDao, 0)

		for res := range ch {
			resData = append(resData, res)
		}

		resp := &responseJson{}
		resp.Data = resData
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
