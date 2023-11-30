package infrastructures

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"os"
	"parquet_2/models"
)

func NewS3Handler(cfg aws.Config) *S3Handler {
	client := s3.NewFromConfig(cfg)

	return &S3Handler{
		client: client,
	}
}

type S3Handler struct {
	client *s3.Client
}

func (handler *S3Handler) ListFiles(bucket string) []models.S3RemoteFile {
	ctx := context.Background()

	listObjectsV2Input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(2),
	}

	s3Files := make([]models.S3RemoteFile, 0)

	result, err := handler.client.ListObjectsV2(ctx, listObjectsV2Input)
	if err != nil {
		//if aerr, ok := err.(awserr.Error); ok {
		//	switch aerr.Code() {
		//	case s3.ErrCodeNoSuchBucket:
		//		fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
		//	default:
		//		fmt.Println(aerr.Error())
		//	}
		//} else {
		//	// Print the error, cast err to awserr.Error to get the Code and
		//	// Message from an error.
		//	fmt.Println(err.Error())
		//}
		//
		//fmt.Println(err.Error())
		return s3Files
	}

	for _, key := range result.Contents {
		s3Files = append(s3Files, models.S3RemoteFile{
			Name:         *key.Key,
			ModifiedDate: *key.LastModified,
		})
	}

	return s3Files
}

func (handler *S3Handler) DownloadFile(bucket, keyFile, dest string) bool {
	ctx := context.Background()

	if _, err := os.Stat(dest); err == nil {
		os.Remove(dest)
	}

	downloadFile, err := os.Create(dest)
	if err != nil {
		log.Println("Failed opening file", dest, err)
	}
	defer downloadFile.Close()

	downloader := manager.NewDownloader(handler.client)
	_, err = downloader.Download(ctx, downloadFile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(keyFile),
	})

	if err != nil {
		log.Fatalln("Failed to download file ", keyFile, err)

		return false
	}

	return true
}
