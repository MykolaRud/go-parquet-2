package repositories

import (
	"database/sql"
	"fmt"
	"parquet_2/interfaces"
	"parquet_2/models"
)

type DBFilesRepository struct {
	DB interfaces.IDbHandler
}

func (repo *DBFilesRepository) FileExists(remoteFile models.S3RemoteFile) bool {
	var rowId int64

	sqlDate := remoteFile.ModifiedDate.Format("2006-01-02 15:04:05")

	row := repo.DB.QueryRow("SELECT id FROM s3_files WHERE s3_files.`name` = ? AND last_modified_at= ? AND  is_processed = 1", remoteFile.Name, sqlDate)
	err := row.Scan(&rowId)
	if err == sql.ErrNoRows {
		return false
	}

	return true
}

func (repo *DBFilesRepository) SetAsProcessed(remoteFile models.S3RemoteFile) {
	sqlDate := remoteFile.ModifiedDate.Format("2006-01-02 15:04:05")

	repo.DB.Execute("INSERT INTO s3_files (`name`, last_modified_at, is_processed) VALUES (?, ?, 1)", remoteFile.Name, sqlDate)
}

func (repo *DBFilesRepository) ArtistExists(artistId int64) bool {
	var existingArtist models.DBArtist

	row := repo.DB.QueryRow("SELECT * FROM artists_balance WHERE artists_balance.artist_id = ?", artistId)
	err := row.Scan(&existingArtist)
	if err == sql.ErrNoRows {
		return false
	}

	return true
}

func (repo *DBFilesRepository) CreateOrUpdateArtist(artistId int64, amount int64) {
	if repo.ArtistExists(artistId) {
		repo.AddArtistBalance(artistId, amount)
	} else {
		repo.CreateArtist(artistId, amount)
	}
}

func (repo *DBFilesRepository) CreateArtist(artistId int64, amount int64) {
	_, err := repo.DB.Execute("INSERT INTO artists_balance (artist_id, balance) VALUES (?,?)", artistId, amount)
	if err != nil {
		fmt.Println("Error creating artists_balance ", err.Error())
	}
}

func (repo *DBFilesRepository) AddArtistBalance(artistId int64, amount int64) {
	_, err := repo.DB.Execute("UPDATE artists_balance SET balance=balance+? WHERE artists_balance.artist_id = ?", amount, artistId)
	if err != nil {
		fmt.Println("Error updating artists_balance ", err.Error())
	}
}
