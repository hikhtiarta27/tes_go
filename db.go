package main

import (
	"database/sql"
	"log"
)

func dbConn() *sql.DB {
	db, err := sql.Open("godror", `user="jne" password="JNEmerdeka123!" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=34.101.218.194)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=pdbdev)))"`)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
