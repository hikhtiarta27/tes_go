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
	Transaction       map[string]int `json:"transaction"`
	TransactionDetail map[string]int `json:"transactionDetail"`
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

func reconstruct(awb *AWBDetail) string {
	sql := "CALL P_DWH_SYNC_CS3_API('" + awb.CNOTE_NO + "'" +
		",TO_DATE('" + awb.CNOTE_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",TO_DATE('" + awb.CNOTE_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.CNOTE_BRANCH_ID + "'" +
		",'" + awb.BRANCH_REGION + "'" +
		",'" + awb.CNOTE_ORIGIN + "'" +
		",'" + awb.ORIGIN_NAME + "'" +
		",'" + awb.ORIGIN_ZONE + "'" +
		",'" + awb.CNOTE_CUST_NO + "'" +
		",'" + awb.CNOTE_CUST_TYPE + "'" +
		",'" + awb.CUST_NAME + "'" +
		",'" + awb.CUST_ADDR1 + "'" +
		",'" + awb.CUST_ADDR2 + "'" +
		",'" + awb.CUST_ADDR3 + "'" +
		",'" + awb.CUST_PHONE + "'" +
		",'" + awb.CUST_ZIP + "'" +
		",'" + awb.CUST_NA + "'" +
		",'" + awb.MARKETPLACE_TYPE + "'" +
		",'" + awb.MARKETPLACE_NAME + "'" +
		",'" + awb.CNOTE_SHIPPER_NAME + "'" +
		",'" + awb.CNOTE_SHIPPER_CONTACT + "'" +
		",'" + awb.CNOTE_SHIPPER_ADDR1 + "'" +
		",'" + awb.CNOTE_SHIPPER_ADDR2 + "'" +
		",'" + awb.CNOTE_SHIPPER_ADDR3 + "'" +
		",'" + awb.CNOTE_SHIPPER_PHONE + "'" +
		",'" + awb.CNOTE_SHIPPER_ZIP + "'" +
		",'" + awb.CNOTE_RECEIVER_NAME + "'" +
		",'" + awb.CNOTE_RECEIVER_CONTACT + "'" +
		",'" + awb.CNOTE_RECEIVER_ADDR1 + "'" +
		",'" + awb.CNOTE_RECEIVER_ADDR2 + "'" +
		",'" + awb.CNOTE_RECEIVER_ADDR3 + "'" +
		",'" + awb.CNOTE_RECEIVER_PHONE + "'" +
		",'" + awb.CNOTE_RECEIVER_ZIP + "'" +
		",'" + awb.CNOTE_DESTINATION_ID + "'" +
		",'" + awb.BRANCH_DEST_REGION + "'" +
		",'" + awb.BRANCH_DESTINATION + "'" +
		",'" + awb.CNOTE_DESTINATION + "'" +
		",'" + awb.DESTINATION_NAME + "'" +
		",'" + awb.DESTINATION_CODE + "'" +
		",'" + awb.DESTINATION_ZONE + "'" +
		",'" + awb.CNOTE_SERVICES_CODE + "'" +
		",'" + awb.ROUTE_ETD_FROM + "'" +
		",'" + awb.ROUTE_ETD_THRU + "'" +
		",'" + awb.CNOTE_SHIPMENT_TYPE + "'" +
		",'" + awb.CNOTE_TRX_TYPE + "'" +
		",'" + awb.CNOTE_PAYMENT_TYPE + "'" +
		",'" + awb.CNOTE_QTY + "'" +
		",'" + awb.CNOTE_WEIGHT + "'" +
		",'" + awb.CNOTE_DIM + "'" +
		",'" + awb.CNOTE_GOODS_TYPE + "'" +
		",'" + awb.CNOTE_PACKING + "'" +
		",'" + awb.CNOTE_GOODS_DESCR + "'" +
		",'" + awb.CNOTE_GOODS_VALUE + "'" +
		",'" + awb.CNOTE_SPECIAL_INS + "'" +
		",'" + awb.CNOTE_INSURANCE_ID + "'" +
		",'" + awb.CNOTE_INSURANCE_VALUE + "'" +
		",'" + awb.CNOTE_AMOUNT + "'" +
		",'" + awb.CNOTE_ADDITIONAL_FEE + "'" +
		",'" + awb.CNOTE_COD + "'" +
		",'" + awb.COD_NO + "'" +
		",'" + awb.COD_GOODS_AMOUNT + "'" +
		",'" + awb.COD_AMOUNT + "'" +
		",'" + awb.CNOTE_CASHLESS + "'" +
		",'" + awb.JLC_NO + "'" +
		",'" + awb.JLC_NAME + "'" +
		",'" + awb.JLC_DISCOUNT + "'" +
		",'" + awb.HYBRID_BRANCH + "'" +
		",'" + awb.HYBRID_CUST_NO + "'" +
		",'" + awb.HYBRID_CUST_NAME + "'" +
		",'" + awb.HYBRID_CUST_ADDR1 + "'" +
		",'" + awb.HYBRID_CUST_ADDR2 + "'" +
		",'" + awb.HYBRID_CUST_ADDR3 + "'" +
		",'" + awb.HYBRID_CUST_PHONE + "'" +
		",'" + awb.HYBRID_CUST_ZIP + "'" +
		",'" + awb.CNOTE_CANCEL + "'" +
		",'" + awb.CNOTE_HOLD + "'" +
		",'" + awb.CNOTE_USER + "'" +
		",'" + awb.CNOTE_USER_ZONE + "'" +
		",'" + awb.R_CNOTE_FREIGHT_CHARGE + "'" +
		",'" + awb.PUBLISH_RATE + "'" +
		",'" + awb.CASHREG_NO + "'" +
		",TO_DATE('" + awb.CASHREG_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.CASHREG_USER_ID + "'" +
		",'" + awb.CASHREG_USER_ZONE + "'" +
		",TO_DATE('" + awb.CASHREG_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PICKUP_NO + "'" +
		",'" + awb.PICKUP_COURIER_ID + "'" +
		",'" + awb.PICKUP_COURIER_ZONE + "'" +
		",TO_DATE('" + awb.PICKUP_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",TO_DATE('" + awb.PICKUP_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PICKUP_MERCHAN_ID + "'" +
		",'" + awb.PICKUP_LATITUDE + "'" +
		",'" + awb.PICKUP_LONGITUDE + "'" +
		",'" + awb.PU_FIRST_ATTTEMP_STATUS_CODE + "'" +
		",'" + awb.PU_FIRST_ATTTEMP_STATUS_DESC + "'" +
		",TO_DATE('" + awb.PU_FIRST_ATTTEMP_STATUS_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PU_LAST_ATTEMP_STATUS_CODE + "'" +
		",'" + awb.PU_LAST_ATTEMP_STATUS_DESC + "'" +
		",TO_DATE('" + awb.PU_LAST_ATTEMP_STATUS_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PU_REF_ID + "'" +
		",'" + awb.HO_NO + "'" +
		",TO_DATE('" + awb.HO_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.HO_COURIER_ID + "'" +
		",TO_DATE('" + awb.HO_CDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.RECEIVING_AGENT_NO + "'" +
		",TO_DATE('" + awb.RECEIVING_AGENT_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.RECEIVING_AGENT_BRANCH + "'" +
		",'" + awb.RECEIVING_AGENT_COURIER_ID + "'" +
		",'" + awb.RECEIVING_AGENT_USER_ID + "'" +
		",'" + awb.RECEIVING_AGENT_USER_ZONE + "'" +
		",TO_DATE('" + awb.RECEIVING_AGENT_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.RECEIVING_OUT_NO + "'" +
		",TO_DATE('" + awb.RECEIVING_OUT_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.RECEIVING_OUT_BRANCH + "'" +
		",'" + awb.RECEIVING_OUT_COURIER_ID + "'" +
		",'" + awb.RECEIVING_OUT_USER_ID + "'" +
		",'" + awb.RECEIVING_OUT_USER_ZONE + "'" +
		",TO_DATE('" + awb.RECEIVING_OUT_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_OUTB_NO + "'" +
		",'" + awb.MANIFEST_OUTB_ORIGIN + "'" +
		",TO_DATE('" + awb.MANIFEST_OUTB_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_OUTB_BAG_NO + "'" +
		",'" + awb.MANIFEST_OUTB_USER_ID + "'" +
		",'" + awb.MANIFEST_OUTB_USER_ZONE + "'" +
		",TO_DATE('" + awb.MANIFEST_OUTB_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.SMU_NO + "'" +
		",'" + awb.SMU_SCHD_NO + "'" +
		",TO_DATE('" + awb.SMU_SCH_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",TO_DATE('" + awb.SMU_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",TO_DATE('" + awb.SMU_ETD + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",TO_DATE('" + awb.SMU_ETA + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.SMU_REMARKS + "'" +
		",TO_DATE('" + awb.SMU_REMARKS_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.SMU_QTY + "'" +
		",'" + awb.SMU_WEIGHT + "'" +
		",'" + awb.SMU_FLAG_APPROVE + "'" +
		",'" + awb.SMU_FLAG_CANCEL + "'" +
		",'" + awb.SMU_DESTINATION + "'" +
		",'" + awb.MANIFEST_TRS1_NO + "'" +
		",'" + awb.MANIFEST_TRS1_ORIGIN + "'" +
		",TO_DATE('" + awb.MANIFEST_TRS1_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_TRS1_BAG_NO + "'" +
		",'" + awb.MANIFEST_TRS1_USER_ID + "'" +
		",'" + awb.MANIFEST_TRS1_USER_ZONE + "'" +
		",TO_DATE('" + awb.MANIFEST_TRS1_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_TRSL_NO + "'" +
		",'" + awb.MANIFEST_TRSL_ORIGIN + "'" +
		",TO_DATE('" + awb.MANIFEST_TRSL_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_TRSL_BAG_NO + "'" +
		",'" + awb.MANIFEST_TRSL_USER_ID + "'" +
		",'" + awb.MANIFEST_TRSL_USER_ZONE + "'" +
		",TO_DATE('" + awb.MANIFEST_TRSL_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_INB_NO + "'" +
		",'" + awb.MANIFEST_INB_ORIGIN + "'" +
		",TO_DATE('" + awb.MANIFEST_INB_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_INB_BAG_NO + "'" +
		",'" + awb.MANIFEST_INB_USER_ID + "'" +
		",'" + awb.MANIFEST_INB_USER_ZONE + "'" +
		",TO_DATE('" + awb.MANIFEST_INB_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_BAG_NO + "'" +
		",TO_DATE('" + awb.MANIFEST_BAG_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MANIFEST_BAG_BAG_NO + "'" +
		",'" + awb.MANIFEST_BAG_USER_ID + "'" +
		",'" + awb.MANIFEST_BAG_USER_ZONE + "'" +
		",TO_DATE('" + awb.MANIFEST_BAG_CRDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PRA_MRSHEET_NO + "'" +
		",TO_DATE('" + awb.PRA_MRSHEET_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PRA_MRSHEET_BRANCH + "'" +
		",'" + awb.PRA_MRSHEET_ZONE + "'" +
		",'" + awb.PRA_MRSHEET_COURIER_ID + "'" +
		",'" + awb.PRA_COURIER_ZONE_CODE + "'" +
		",'" + awb.PRA_MRSHEET_UID + "'" +
		",'" + awb.PRA_USER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.PRA_CREATION_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MTA_OUT_MANIFEST_NO + "'" +
		",TO_DATE('" + awb.MTA_OUT_MANIFEST_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MTA_OUT_BRANCH_ID + "'" +
		",'" + awb.MTA_OUT_DESTINATION + "'" +
		",'" + awb.MTA_OUT_MANIFEST_UID + "'" +
		",'" + awb.MTA_OUT_USER_ZONE_CODE + "'" +
		",'" + awb.MTA_OUT_ESB_TIME + "'" +
		",'" + awb.MTA_INB_MANIFEST_NO + "'" +
		",TO_DATE('" + awb.MTA_INB_MANIFEST_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MTA_INB_BRANCH_ID + "'" +
		",'" + awb.MTA_INB_DESTINATION + "'" +
		",'" + awb.MTA_INB_MANIFEST_UID + "'" +
		",'" + awb.MTA_INB_USER_ZONE_CODE + "'" +
		",'" + awb.MTA_INB_ESB_TIME + "'" +
		",'" + awb.MHOCNOTE_NO + "'" +
		",TO_DATE('" + awb.MHOCNOTE_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MHOCNOTE_BRANCH_ID + "'" +
		",'" + awb.MHOCNOTE_ZONE + "'" +
		",'" + awb.MHOCNOTE_ZONE_DEST + "'" +
		",'" + awb.MHOCNOTE_USER_ID + "'" +
		",'" + awb.MHOCNOTE_USER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.DHOCNOTE_TDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MHICNOTE_NO + "'" +
		",TO_DATE('" + awb.MHICNOTE_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MHICNOTE_BRANCH_ID + "'" +
		",'" + awb.MHICNOTE_ZONE + "'" +
		",'" + awb.MHICNOTE_USER_ID + "'" +
		",'" + awb.MHICNOTE_USER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.DHICNOTE_TDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MRSHEET1_NO + "'" +
		",TO_DATE('" + awb.MRSHEET1_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MRSHEET1_BRANCH + "'" +
		",'" + awb.MRSHEET1_COURIER_ID + "'" +
		",'" + awb.MRSHEET1_UID + "'" +
		",'" + awb.MRSHEET1_USER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.MRSHEET1_CREATION_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MRSHEETL_NO + "'" +
		",TO_DATE('" + awb.MRSHEETL_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.MRSHEETL_BRANCH + "'" +
		",'" + awb.MRSHEETL_COURIER_ID + "'" +
		",'" + awb.MRSHEETL_UID + "'" +
		",'" + awb.MRSHEETL_USER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.MRSHEETL_CREATION_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.POD1_DRSHEET_NO + "'" +
		",TO_DATE('" + awb.POD1_MRSHEET_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.POD1_MRSHEET_BRANCH + "'" +
		",'" + awb.POD1_MRSHEET_COURIER_ID + "'" +
		",'" + awb.POD1_COURIER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.POD1_DRSHEET_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.POD1_DRSHEET_RECEIVER + "'" +
		",'" + awb.POD1_DRSHEET_STATUS + "'" +
		",'" + awb.POD1_LATITUDE + "'" +
		",'" + awb.POD1_LONGITUDE + "'" +
		",'" + awb.POD1_EPOD_URL + "'" +
		",'" + awb.POD1_EPOD_URL_PIC + "'" +
		",'" + awb.POD1_DRSHEET_UID + "'" +
		",'" + awb.POD1_USER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.POD1_DRSHEET_UDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PODL_DRSHEET_NO + "'" +
		",TO_DATE('" + awb.PODL_MRSHEET_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PODL_MRSHEET_BRANCH + "'" +
		",'" + awb.PODL_MRSHEET_COURIER_ID + "'" +
		",'" + awb.PODL_COURIER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.PODL_DRSHEET_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.PODL_DRSHEET_RECEIVER + "'" +
		",'" + awb.PODL_DRSHEET_STATUS + "'" +
		",'" + awb.PODL_LATITUDE + "'" +
		",'" + awb.PODL_LONGITUDE + "'" +
		",'" + awb.PODL_EPOD_URL + "'" +
		",'" + awb.PODL_EPOD_URL_PIC + "'" +
		",'" + awb.PODL_DRSHEET_UID + "'" +
		",'" + awb.PODL_USER_ZONE_CODE + "'" +
		",TO_DATE('" + awb.PODL_DRSHEET_UDATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.DO_NO + "'" +
		",TO_DATE('" + awb.DO_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.RDO_NO + "'" +
		",TO_DATE('" + awb.RDO_DATE + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.SHIPPER_PROVIDER + "'" +
		",'" + awb.CNOTE_REFNO + "'" +
		",'" + awb.MANIFEST_OUTB_APPROVED + "'" +
		",'" + awb.MANIFEST_INB_APPROVED + "'" +
		",'" + awb.SMU_BAG_BUX + "'" +
		",TO_DATE('" + awb.SMU_TGL_MASTER_BAG + "', 'YYYY-MM-DD HH24:MI:SS')" +
		",'" + awb.SMU_USER_MASTER_BAG + "'" +
		",'" + awb.SMU_NO_MASTER_BAG + "'" +
		",'" + awb.SMU_MANIFEST_DESTINATION + "'" +
		",'" + awb.MANIFEST_COST_WEIGHT + "'" +
		",'" + awb.MANIFEST_ACT_WEIGHT + "'" +
		",'" + awb.DWH_PACKING_FEE + "'" +
		",'" + awb.DWH_SURCHARGE + "'" +
		",'" + awb.DWH_DISC_REV_TYPE + "'" +
		",'" + awb.DWH_DISCOUNT_AMT + "'" +
		",'" + awb.DWH_FCHARGE_AFT_DISC_AMT + "'" +
		",'" + awb.DWH_CUST_DISC_IC + "'" +
		",'" + awb.DWH_CUST_DISC_DM + "'" +
		",'" + awb.DWH_RT_PACKING_FEE + "'" +
		",'" + awb.DWH_RT_FREIGHT_CHARGE + "'" +
		",'" + awb.DWH_RT_SURCHARGE + "'" +
		",'" + awb.DWH_RT_DISC_AMT + "'" +
		",'" + awb.DWH_RT_FCHARGE_AFT_DISC_AMT + "'" +
		",'" + awb.DWH_PAYTYPE + "'" +
		",'" + awb.DWH_EPAY_VEND + "'" +
		",'" + awb.DWH_EPAY_TRXID + "'" +
		",'" + awb.DWH_VAT_FCHARGE_AFT_DISC + "'" +
		",'" + awb.DWH_VAT_RT_FCHARGE_AFT_DISC + "')"
	return sql
}

func syncTransaction(ch chan<- map[string]int, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	total, success, failed := 0, 0, 0

	transactionObj := map[string]int{
		"total":   total,
		"success": success,
		"failed":  failed,
	}

	q, err := db.Query("SELECT t.AWB, t.CREATED_DATE_SEARCH, t.SHIPPER_NAME FROM \"TRANSACTION\" t LEFT JOIN T_SUKSES_TERIMA ts ON t.AWB = ts.AWB " +
		"WHERE ts.AWB IS NULL " +
		"AND TRUNC(t.CREATED_DATE_SEARCH) >= TO_DATE('2021-11-21', 'YYYY-MM-DD') " +
		"AND TRUNC(t.CREATED_DATE_SEARCH) <= TO_DATE('2022-01-21', 'YYYY-MM-DD') " +
		"ORDER BY t.CREATED_DATE_SEARCH ASC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")

	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		transaction := new(TransactionDao)
		if err := q.Scan(&transaction.AWB, &transaction.CREATED_DATE_SEARCH, &transaction.SHIPPER_NAME); err != nil {
			log.Fatal(err)
		}

		url := "http://apilazada.jne.co.id:8889/tracing/cs3new/selectDataByCnote"
		payload, _ := json.Marshal(
			map[string]string{
				"cnote": transaction.AWB,
			})

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))

		if err != nil {
			failed++
			log.Fatal(err)
		}

		awb := AWBDetail{}

		json.NewDecoder(resp.Body).Decode(&awb)

		fmt.Println(resp.Body)

		// procedureSql := reconstruct(&awb)

		fmt.Println("Transaction")
		// fmt.Println(procedureSql)
		fmt.Println(total)

		// _, err = db.Exec(procedureSql)

		total++
		// if err != nil {
		// 	failed++
		// 	log.Fatal(err)
		// }

		success++
	}

	ch <- transactionObj
}

func syncTransactionDetail(ch chan<- map[string]int, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	total, success, failed := 0, 0, 0

	transactionDetailObj := map[string]int{
		"total":   total,
		"success": success,
		"failed":  failed,
	}

	q, err := db.Query("SELECT td.AWB_NO, td.AWB_DATE, td.CUST_NAME FROM TRANSACTION_DETAIL td " +
		"LEFT JOIN \"TRANSACTION\" t ON td.AWB_NO = t.AWB " +
		"LEFT JOIN T_SUKSES_TERIMA ts ON td.AWB_NO = ts.AWB " +
		"WHERE t.AWB IS NULL AND ts.AWB IS NULL " +
		"AND TRUNC(td.AWB_DATE) >= TO_DATE('2021-11-21', 'YYYY-MM-DD') " +
		"AND TRUNC(td.AWB_DATE) <= TO_DATE('2022-01-21', 'YYYY-MM-DD') " +
		"ORDER BY td.AWB_DATE OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY")
	if err != nil {
		log.Fatal(err)
	}

	for q.Next() {
		transaction := new(TransactionDao)
		if err := q.Scan(&transaction.AWB, &transaction.CREATED_DATE_SEARCH, &transaction.SHIPPER_NAME); err != nil {
			log.Fatal(err)
		}

		url := "http://apilazada.jne.co.id:8889/tracing/cs3new/selectDataByCnote"
		payload, _ := json.Marshal(
			map[string]string{
				"cnote": transaction.AWB,
			})

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))

		if err != nil {
			failed++
			log.Fatal(err)
		}

		awb := AWBDetail{}

		json.NewDecoder(resp.Body).Decode(&awb)

		fmt.Println(awb)

		// procedureSql := reconstruct(&awb)

		fmt.Println("Transaction Detail")
		// fmt.Println(procedureSql)

		fmt.Println(total)

		// _, err = db.Exec(procedureSql)

		total++

		// if err != nil {
		// 	failed++
		// 	log.Fatal(err)
		// }

		success++
	}

	ch <- transactionDetailObj
}

func main() {

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/synchronize", func(w http.ResponseWriter, r *http.Request) {

		db, _ := sql.Open("godror", `user="jne" password="JNEmerdeka123!" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=34.101.218.194)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbprod)))"`)
		db.SetMaxOpenConns(50)

		ch := make(chan map[string]int)
		ch1 := make(chan map[string]int)

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
