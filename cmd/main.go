package main

import (
	"parquet_2/handlers"
	"parquet_2/infrastructures"
	"time"
)

func main() {
	var refreshDataHandler *handlers.RefreshDataHandler

	connection := infrastructures.InitDBConnection(infrastructures.GetMySQLConfig())
	mySQL := infrastructures.NewMySQLHandler(connection)

	s3Config := infrastructures.GetS3ConnectionConfig()
	s3 := infrastructures.NewS3Handler(s3Config)

	refreshDataHandler = handlers.NewRefreshDataHandler(mySQL, s3, infrastructures.GetConfigBucketName())

	for {
		refreshDataHandler.Run()
		time.Sleep(time.Second * 3)
	}

	connection.Close()
}
