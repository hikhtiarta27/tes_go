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

type responseData struct {
	Transaction       [][]*TransactionDao `json:"transaction"`
	TransactionDetail [][]*TransactionDao `json:"transactionDetail"`
}
type responseJson struct {
	Success bool         `json:"status"`
	Message string       `json:"message"`
	Data    responseData `json:"data"`
}

type TransactionDao struct {
	AWB                 string
	CREATED_DATE_SEARCH string
	SHIPPER_NAME        string
}

func syncTransaction(ch chan<- []*TransactionDao, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	transactionList := make([]*TransactionDao, 0)

	q, err := db.Query("SELECT t.AWB, t.CREATED_DATE_SEARCH, t.SHIPPER_NAME FROM \"TRANSACTION\" t LEFT JOIN T_SUKSES_TERIMA ts ON t.AWB = ts.AWB " +
		"WHERE TRUNC(t.CREATED_DATE_SEARCH) >= TO_DATE('2021-01-01', 'YYYY-MM-DD') " +
		"AND TRUNC(t.CREATED_DATE_SEARCH) <= TO_DATE('2021-12-31','YYYY-MM-DD') AND ts.AWB != NULL ORDER BY t.CREATED_DATE_SEARCH ASC")
	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		transaction := new(TransactionDao)
		if err := q.Scan(&transaction.AWB, &transaction.CREATED_DATE_SEARCH, &transaction.SHIPPER_NAME); err != nil {
			log.Fatal(err)
		}
		transactionList = append(transactionList, transaction)
	}

	ch <- transactionList
}

func syncTransactionDetail(ch chan<- []*TransactionDao, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	transactionList := make([]*TransactionDao, 0)

	q, err := db.Query("SELECT t.AWB, t.CREATED_DATE_SEARCH, t.SHIPPER_NAME FROM \"TRANSACTION\" t LEFT JOIN T_SUKSES_TERIMA ts ON t.AWB = ts.AWB " +
		"WHERE TRUNC(t.CREATED_DATE_SEARCH) >= TO_DATE('2021-01-01', 'YYYY-MM-DD') " +
		"AND TRUNC(t.CREATED_DATE_SEARCH) <= TO_DATE('2021-12-31','YYYY-MM-DD') AND ts.AWB != NULL ORDER BY t.CREATED_DATE_SEARCH ASC")
	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		transaction := new(TransactionDao)
		if err := q.Scan(&transaction.AWB, &transaction.CREATED_DATE_SEARCH, &transaction.SHIPPER_NAME); err != nil {
			log.Fatal(err)
		}
		transactionList = append(transactionList, transaction)
	}

	ch <- transactionList
}

func main() {

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/synchronize", func(w http.ResponseWriter, r *http.Request) {

		db, _ := sql.Open("godror", `user="jne" password="JNEmerdeka123!" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=34.101.218.194)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbdev)))"`)
		db.SetMaxOpenConns(50)

		ch := make(chan []*TransactionDao)
		ch1 := make(chan []*TransactionDao)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			go syncTransaction(ch, &wg, db)
		}()

		wg.Add(1)
		go syncTransactionDetail(ch1, &wg, db)

		// close the channel in the background
		go func() {
			wg.Wait()
			close(ch)
			close(ch1)
		}()

		// read from channel as they come in until its closed

		resTransaction := make([][]*TransactionDao, 0)

		for res := range ch {
			resTransaction = append(resTransaction, res)
		}

		resTransactionDetail := make([][]*TransactionDao, 0)

		for res := range ch {
			resTransactionDetail = append(resTransactionDetail, res)
		}

		respData := &responseData{
			Transaction:       resTransaction,
			TransactionDetail: resTransactionDetail,
		}

		resp := &responseJson{}
		resp.Data = *respData
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
