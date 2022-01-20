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
	TOTAL        int
	TOTAL_COD    int
	TOTAL_NONCOD int
}

func callApi(userId int, ch chan<- []*TicketCategoryDao, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	ticketCategoryList := make([]*TicketCategoryDao, 0)

	q, err := db.Query("SELECT COUNT(TTT.AWB) TOTAL, NVL(SUM(TTT.COD_FLAG),0) TOTAL_COD, NVL(COUNT(TTT.AWB) - SUM(TTT.COD_FLAG),0) TOTAL_NONCOD FROM T_TOTAL_TRANS TTT, TRANSACTION T WHERE TTT.AWB = T.AWB AND REGISTRATION_ID = '21100710222523'")
	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		ticketCategory := new(TicketCategoryDao)
		if err := q.Scan(&ticketCategory.TOTAL, &ticketCategory.TOTAL_COD, &ticketCategory.TOTAL_NONCOD); err != nil {
			log.Fatal(err)
		}

		ticketCategoryList = append(ticketCategoryList, ticketCategory)

	}

	q, err = db.Query("SELECT COUNT(TMK.AWB) TOTAL, NVL(SUM(TMK.COD_FLAG),0) TOTAL_COD, NVL(COUNT(TMK.AWB) - SUM(TMK.COD_FLAG),0) TOTAL_NONCOD FROM T_MSH_KAMU TMK, TRANSACTION T WHERE TMK.AWB = T.AWB AND REGISTRATION_ID = '21100710222523'")
	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		ticketCategory := new(TicketCategoryDao)
		if err := q.Scan(&ticketCategory.TOTAL, &ticketCategory.TOTAL_COD, &ticketCategory.TOTAL_NONCOD); err != nil {
			log.Fatal(err)
		}

		ticketCategoryList = append(ticketCategoryList, ticketCategory)

	}

	q, err = db.Query("SELECT COUNT(TSJ.AWB) TOTAL, NVL(SUM(TSJ.COD_FLAG),0) TOTAL_COD, NVL(COUNT(TSJ.AWB) - SUM(TSJ.COD_FLAG),0) TOTAL_NONCOD FROM T_SDH_JEMPUT TSJ, TRANSACTION T WHERE TSJ.AWB = T.AWB AND REGISTRATION_ID = '21100710222523'")
	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		ticketCategory := new(TicketCategoryDao)
		if err := q.Scan(&ticketCategory.TOTAL, &ticketCategory.TOTAL_COD, &ticketCategory.TOTAL_NONCOD); err != nil {
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

	r.Get("/synchronize", func(w http.ResponseWriter, r *http.Request) {

		db, _ := sql.Open("godror", `user="jne" password="JNEmerdeka123!" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=34.101.218.194)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbdev)))"`)

		ch := make(chan []*TicketCategoryDao)

		var wg sync.WaitGroup

		for i := 0; i < 1; i++ {
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
