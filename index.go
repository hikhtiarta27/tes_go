package main

import (
	"bytes"
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
	Transaction       []*TransactionDao `json:"transaction"`
	TransactionDetail []*TransactionDao `json:"transactionDetail"`
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

type AWBDetail struct {
	CREATE_DATE                  string
	CNOTE_NO                     string
	CNOTE_DATE                   string
	CNOTE_CRDATE                 string
	CNOTE_BRANCH_ID              string
	BRANCH_REGION                string
	CNOTE_ORIGIN                 string
	ORIGIN_NAME                  string
	ORIGIN_ZONE                  string
	CNOTE_CUST_NO                string
	CNOTE_CUST_TYPE              string
	CUST_NAME                    string
	CUST_ADDR1                   string
	CUST_ADDR2                   string
	CUST_ADDR3                   string
	CUST_PHONE                   string
	CUST_ZIP                     string
	CUST_NA                      string
	MARKETPLACE_TYPE             string
	MARKETPLACE_NAME             string
	CNOTE_SHIPPER_NAME           string
	CNOTE_SHIPPER_CONTACT        string
	CNOTE_SHIPPER_ADDR1          string
	CNOTE_SHIPPER_ADDR2          string
	CNOTE_SHIPPER_ADDR3          string
	CNOTE_SHIPPER_PHONE          string
	CNOTE_SHIPPER_ZIP            string
	CNOTE_RECEIVER_NAME          string
	CNOTE_RECEIVER_CONTACT       string
	CNOTE_RECEIVER_ADDR1         string
	CNOTE_RECEIVER_ADDR2         string
	CNOTE_RECEIVER_ADDR3         string
	CNOTE_RECEIVER_PHONE         string
	CNOTE_RECEIVER_ZIP           string
	CNOTE_DESTINATION_ID         string
	BRANCH_DEST_REGION           string
	BRANCH_DESTINATION           string
	CNOTE_DESTINATION            string
	DESTINATION_NAME             string
	DESTINATION_CODE             string
	DESTINATION_ZONE             string
	CNOTE_SERVICES_CODE          string
	ROUTE_ETD_FROM               string
	ROUTE_ETD_THRU               string
	CNOTE_SHIPMENT_TYPE          string
	CNOTE_TRX_TYPE               string
	CNOTE_PAYMENT_TYPE           string
	CNOTE_QTY                    string
	CNOTE_WEIGHT                 string
	CNOTE_DIM                    string
	CNOTE_GOODS_TYPE             string
	CNOTE_PACKING                string
	CNOTE_GOODS_DESCR            string
	CNOTE_GOODS_VALUE            string
	CNOTE_SPECIAL_INS            string
	CNOTE_INSURANCE_ID           string
	CNOTE_INSURANCE_VALUE        string
	CNOTE_AMOUNT                 string
	CNOTE_ADDITIONAL_FEE         string
	CNOTE_COD                    string
	COD_NO                       string
	COD_GOODS_AMOUNT             string
	COD_AMOUNT                   string
	CNOTE_CASHLESS               string
	JLC_NO                       string
	JLC_NAME                     string
	JLC_DISCOUNT                 string
	HYBRID_BRANCH                string
	HYBRID_CUST_NO               string
	HYBRID_CUST_NAME             string
	HYBRID_CUST_ADDR1            string
	HYBRID_CUST_ADDR2            string
	HYBRID_CUST_ADDR3            string
	HYBRID_CUST_PHONE            string
	HYBRID_CUST_ZIP              string
	CNOTE_CANCEL                 string
	CNOTE_HOLD                   string
	CNOTE_USER                   string
	CNOTE_USER_ZONE              string
	R_CNOTE_FREIGHT_CHARGE       string
	PUBLISH_RATE                 string
	CASHREG_NO                   string
	CASHREG_DATE                 string
	CASHREG_USER_ID              string
	CASHREG_USER_ZONE            string
	CASHREG_CRDATE               string
	PICKUP_NO                    string
	PICKUP_COURIER_ID            string
	PICKUP_COURIER_ZONE          string
	PICKUP_DATE                  string
	PICKUP_CRDATE                string
	PICKUP_MERCHAN_ID            string
	PICKUP_LATITUDE              string
	PICKUP_LONGITUDE             string
	PU_FIRST_ATTTEMP_STATUS_CODE string
	PU_FIRST_ATTTEMP_STATUS_DESC string
	PU_FIRST_ATTTEMP_STATUS_DATE string
	PU_LAST_ATTEMP_STATUS_CODE   string
	PU_LAST_ATTEMP_STATUS_DESC   string
	PU_LAST_ATTEMP_STATUS_DATE   string
	PU_REF_ID                    string
	HO_NO                        string
	HO_DATE                      string
	HO_COURIER_ID                string
	HO_CDATE                     string
	RECEIVING_AGENT_NO           string
	RECEIVING_AGENT_DATE         string
	RECEIVING_AGENT_BRANCH       string
	RECEIVING_AGENT_COURIER_ID   string
	RECEIVING_AGENT_USER_ID      string
	RECEIVING_AGENT_USER_ZONE    string
	RECEIVING_AGENT_CRDATE       string
	RECEIVING_OUT_NO             string
	RECEIVING_OUT_DATE           string
	RECEIVING_OUT_BRANCH         string
	RECEIVING_OUT_COURIER_ID     string
	RECEIVING_OUT_USER_ID        string
	RECEIVING_OUT_USER_ZONE      string
	RECEIVING_OUT_CRDATE         string
	MANIFEST_OUTB_NO             string
	MANIFEST_OUTB_ORIGIN         string
	MANIFEST_OUTB_DATE           string
	MANIFEST_OUTB_BAG_NO         string
	MANIFEST_OUTB_USER_ID        string
	MANIFEST_OUTB_USER_ZONE      string
	MANIFEST_OUTB_CRDATE         string
	SMU_NO                       string
	SMU_SCHD_NO                  string
	SMU_SCH_DATE                 string
	SMU_DATE                     string
	SMU_ETD                      string
	SMU_ETA                      string
	SMU_REMARKS                  string
	SMU_REMARKS_DATE             string
	SMU_QTY                      string
	SMU_WEIGHT                   string
	SMU_FLAG_APPROVE             string
	SMU_FLAG_CANCEL              string
	SMU_DESTINATION              string
	MANIFEST_TRS1_NO             string
	MANIFEST_TRS1_ORIGIN         string
	MANIFEST_TRS1_DATE           string
	MANIFEST_TRS1_BAG_NO         string
	MANIFEST_TRS1_USER_ID        string
	MANIFEST_TRS1_USER_ZONE      string
	MANIFEST_TRS1_CRDATE         string
	MANIFEST_TRSL_NO             string
	MANIFEST_TRSL_ORIGIN         string
	MANIFEST_TRSL_DATE           string
	MANIFEST_TRSL_BAG_NO         string
	MANIFEST_TRSL_USER_ID        string
	MANIFEST_TRSL_USER_ZONE      string
	MANIFEST_TRSL_CRDATE         string
	MANIFEST_INB_NO              string
	MANIFEST_INB_ORIGIN          string
	MANIFEST_INB_DATE            string
	MANIFEST_INB_BAG_NO          string
	MANIFEST_INB_USER_ID         string
	MANIFEST_INB_USER_ZONE       string
	MANIFEST_INB_CRDATE          string
	MANIFEST_BAG_NO              string
	MANIFEST_BAG_DATE            string
	MANIFEST_BAG_BAG_NO          string
	MANIFEST_BAG_USER_ID         string
	MANIFEST_BAG_USER_ZONE       string
	MANIFEST_BAG_CRDATE          string
	PRA_MRSHEET_NO               string
	PRA_MRSHEET_DATE             string
	PRA_MRSHEET_BRANCH           string
	PRA_MRSHEET_ZONE             string
	PRA_MRSHEET_COURIER_ID       string
	PRA_COURIER_ZONE_CODE        string
	PRA_MRSHEET_UID              string
	PRA_USER_ZONE_CODE           string
	PRA_CREATION_DATE            string
	MTA_OUT_MANIFEST_NO          string
	MTA_OUT_MANIFEST_DATE        string
	MTA_OUT_BRANCH_ID            string
	MTA_OUT_DESTINATION          string
	MTA_OUT_MANIFEST_UID         string
	MTA_OUT_USER_ZONE_CODE       string
	MTA_OUT_ESB_TIME             string
	MTA_INB_MANIFEST_NO          string
	MTA_INB_MANIFEST_DATE        string
	MTA_INB_BRANCH_ID            string
	MTA_INB_DESTINATION          string
	MTA_INB_MANIFEST_UID         string
	MTA_INB_USER_ZONE_CODE       string
	MTA_INB_ESB_TIME             string
	MHOCNOTE_NO                  string
	MHOCNOTE_DATE                string
	MHOCNOTE_BRANCH_ID           string
	MHOCNOTE_ZONE                string
	MHOCNOTE_ZONE_DEST           string
	MHOCNOTE_USER_ID             string
	MHOCNOTE_USER_ZONE_CODE      string
	DHOCNOTE_TDATE               string
	MHICNOTE_NO                  string
	MHICNOTE_DATE                string
	MHICNOTE_BRANCH_ID           string
	MHICNOTE_ZONE                string
	MHICNOTE_USER_ID             string
	MHICNOTE_USER_ZONE_CODE      string
	DHICNOTE_TDATE               string
	MRSHEET1_NO                  string
	MRSHEET1_DATE                string
	MRSHEET1_BRANCH              string
	MRSHEET1_COURIER_ID          string
	MRSHEET1_UID                 string
	MRSHEET1_USER_ZONE_CODE      string
	MRSHEET1_CREATION_DATE       string
	MRSHEETL_NO                  string
	MRSHEETL_DATE                string
	MRSHEETL_BRANCH              string
	MRSHEETL_COURIER_ID          string
	MRSHEETL_UID                 string
	MRSHEETL_USER_ZONE_CODE      string
	MRSHEETL_CREATION_DATE       string
	POD1_DRSHEET_NO              string
	POD1_MRSHEET_DATE            string
	POD1_MRSHEET_BRANCH          string
	POD1_MRSHEET_COURIER_ID      string
	POD1_COURIER_ZONE_CODE       string
	POD1_DRSHEET_DATE            string
	POD1_DRSHEET_RECEIVER        string
	POD1_DRSHEET_STATUS          string
	POD1_LATITUDE                string
	POD1_LONGITUDE               string
	POD1_EPOD_URL                string
	POD1_EPOD_URL_PIC            string
	POD1_DRSHEET_UID             string
	POD1_USER_ZONE_CODE          string
	POD1_DRSHEET_UDATE           string
	PODL_DRSHEET_NO              string
	PODL_MRSHEET_DATE            string
	PODL_MRSHEET_BRANCH          string
	PODL_MRSHEET_COURIER_ID      string
	PODL_COURIER_ZONE_CODE       string
	PODL_DRSHEET_DATE            string
	PODL_DRSHEET_RECEIVER        string
	PODL_DRSHEET_STATUS          string
	PODL_LATITUDE                string
	PODL_LONGITUDE               string
	PODL_EPOD_URL                string
	PODL_EPOD_URL_PIC            string
	PODL_DRSHEET_UID             string
	PODL_USER_ZONE_CODE          string
	PODL_DRSHEET_UDATE           string
	DO_NO                        string
	DO_DATE                      string
	RDO_NO                       string
	RDO_DATE                     string
	SHIPPER_PROVIDER             string
	CNOTE_REFNO                  string
	MANIFEST_OUTB_APPROVED       string
	MANIFEST_INB_APPROVED        string
	SMU_BAG_BUX                  string
	SMU_TGL_MASTER_BAG           string
	SMU_USER_MASTER_BAG          string
	SMU_NO_MASTER_BAG            string
	SMU_MANIFEST_DESTINATION     string
	MANIFEST_COST_WEIGHT         string
	MANIFEST_ACT_WEIGHT          string
	DWH_PACKING_FEE              string
	DWH_SURCHARGE                string
	DWH_DISC_REV_TYPE            string
	DWH_DISCOUNT_AMT             string
	DWH_FCHARGE_AFT_DISC_AMT     string
	DWH_CUST_DISC_IC             string
	DWH_CUST_DISC_DM             string
	DWH_RT_PACKING_FEE           string
	DWH_RT_FREIGHT_CHARGE        string
	DWH_RT_SURCHARGE             string
	DWH_RT_DISC_AMT              string
	DWH_RT_FCHARGE_AFT_DISC_AMT  string
	DWH_PAYTYPE                  string
	DWH_EPAY_VEND                string
	DWH_EPAY_TRXID               string
	DWH_VAT_FCHARGE_AFT_DISC     string
	DWH_VAT_RT_FCHARGE_AFT_DISC  string
}

func syncTransaction(ch chan<- []*TransactionDao, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	transactionList := make([]*TransactionDao, 0)

	// q, err := db.Query("SELECT t.AWB, t.CREATED_DATE_SEARCH, t.SHIPPER_NAME FROM \"TRANSACTION\" t LEFT JOIN T_SUKSES_TERIMA ts ON t.AWB = ts.AWB " +
	// 	"WHERE TRUNC(t.CREATED_DATE_SEARCH) >= TO_DATE('2021-01-01', 'YYYY-MM-DD') " +
	// 	"AND TRUNC(t.CREATED_DATE_SEARCH) <= TO_DATE('2021-12-31','YYYY-MM-DD') AND ts.AWB != NULL ORDER BY t.CREATED_DATE_SEARCH ASC")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// for q.Next() {
	transaction := new(TransactionDao)
	transaction.AWB = "CSS5321000017413"
	// if err := q.Scan(&transaction.AWB, &transaction.CREATED_DATE_SEARCH, &transaction.SHIPPER_NAME); err != nil {
	// 	log.Fatal(err)
	// }

	url := "http://apilazada.jne.co.id:8889/tracing/cs3new/selectDataByCnote"
	payload, _ := json.Marshal(
		map[string]string{
			"cnote": transaction.AWB,
		})

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))

	if err != nil {
		log.Fatal(err)
	}

	awb := AWBDetail{}

	json.NewDecoder(resp.Body).Decode(&awb)

	fmt.Println(awb)

	transactionList = append(transactionList, transaction)
	// }

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

		wg.Add(2)
		go syncTransaction(ch, &wg, db)
		go syncTransactionDetail(ch1, &wg, db)

		// close the channel in the background
		go func() {
			wg.Wait()
			close(ch)
			close(ch1)
		}()

		resTransaction := <-ch
		resTransactionDetail := <-ch1

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
