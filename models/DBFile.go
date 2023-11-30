package models

import "time"

type S3File struct {
	Id             int64
	Name           string
	LastModifiedAt time.Time
	IsProcesssed   bool
}
