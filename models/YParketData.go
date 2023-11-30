package models

type YParketData struct {
	Artist string `parquet:"name=artist, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Amount int64  `parquet:"name=amount, type=INT64, convertedtype=INT_64, repetitiontype=OPTIONAL"`
}
