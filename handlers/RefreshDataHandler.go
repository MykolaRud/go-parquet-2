package handlers

import (
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"log"
	"math"
	"parquet_2/infrastructures"
	"parquet_2/interfaces"
	"parquet_2/models"
	"parquet_2/repositories"
	"strconv"
)

func NewRefreshDataHandler(dbHandler interfaces.IDbHandler, s3Handler interfaces.IS3Handler, bucketName string) *RefreshDataHandler {

	dbFilesRepo := repositories.DBFilesRepository{dbHandler}
	dbArtistsRepo := repositories.DBArtistsRepository{dbHandler}
	tempFileName := infrastructures.GetTempFileName()

	return &RefreshDataHandler{
		DBHandler:           dbHandler,
		S3Handler:           s3Handler,
		DBFilesRepository:   dbFilesRepo,
		DBArtistsRepository: dbArtistsRepo,
		BucketName:          bucketName,
		TempFileName:        tempFileName,
	}
}

type RefreshDataHandler struct {
	DBHandler           interfaces.IDbHandler
	S3Handler           interfaces.IS3Handler
	DBFilesRepository   repositories.DBFilesRepository
	DBArtistsRepository repositories.DBArtistsRepository
	BucketName          string
	TempFileName        string
}

func (h *RefreshDataHandler) Run() {
	fmt.Println("sync start")

	//get s3 list of files
	files := h.S3Handler.ListFiles(h.BucketName)

	//check with DB if there are new files
	newFiles := h.FilterNewFiles(files)

	for _, file := range newFiles {
		//download file
		if !h.S3Handler.DownloadFile(h.BucketName, file.Name, h.TempFileName) {
			fmt.Println("Error downloading file " + file.Name)

			continue
		}

		//process file
		artistRows := h.ProcessParquetFile()
		h.UpdateArtistsData(artistRows)

		//mark as processed to DB
		h.DBFilesRepository.SetAsProcessed(file)
	}

	fmt.Println("sync end")
}
func (h *RefreshDataHandler) FilterNewFiles(files []models.S3RemoteFile) []models.S3RemoteFile {
	filteredFiles := make([]models.S3RemoteFile, 0)
	for _, file := range files {
		if !h.DBFilesRepository.FileExists(file) {
			filteredFiles = append(filteredFiles, file)
		}
	}

	return filteredFiles
}

func (h *RefreshDataHandler) ProcessParquetFile() map[string]int64 {
	resultSet := make(map[string]int64)
	//fill data
	fr, err := local.NewLocalFileReader(h.TempFileName)
	if err != nil {
		log.Println("Can't open file ", h.TempFileName)
		return resultSet
	}

	pr, err := reader.NewParquetReader(fr, new(models.YParketData), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return resultSet
	}
	num := int(pr.GetNumRows())

	dataBatchSize := 10
	maxIteration := int(math.Ceil(float64(num) / float64(dataBatchSize)))
	for i := 0; i < maxIteration; i++ {
		rows := make([]models.YParketData, dataBatchSize)
		if err = pr.Read(&rows); err != nil {
			log.Println("Read error", err)

			continue
		}

		for _, row := range rows {
			resultSet[row.Artist] = resultSet[row.Artist] + row.Amount
		}
	}

	log.Println("Import finished")
	pr.ReadStop()
	fr.Close()

	return resultSet
}

func (h *RefreshDataHandler) UpdateArtistsData(rows map[string]int64) {
	for id, amount := range rows {
		ArtistId, err := strconv.Atoi(id)
		if err != nil {
			fmt.Println("Error ArtistId conversion ", err.Error())
		}

		h.DBArtistsRepository.CreateOrUpdateArtist(int64(ArtistId), amount)
	}
}
