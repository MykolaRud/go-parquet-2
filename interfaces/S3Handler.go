package interfaces

import "parquet_2/models"

type IS3Handler interface {
	ListFiles(bucket string) []models.S3RemoteFile
	DownloadFile(bucket, keyFile, dest string) bool
}
