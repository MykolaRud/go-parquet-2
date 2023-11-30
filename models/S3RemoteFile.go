package models

import (
	"time"
)

type S3RemoteFile struct {
	Name         string
	ModifiedDate time.Time
}
