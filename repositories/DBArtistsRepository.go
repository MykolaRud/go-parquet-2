package repositories

import (
	"database/sql"
	"fmt"
	"parquet_2/interfaces"
	"parquet_2/models"
)

type DBArtistsRepository struct {
	DB interfaces.IDbHandler
}

func (repo *DBArtistsRepository) ArtistExists(artistId int64) bool {
	var existingArtist models.DBArtist

	row := repo.DB.QueryRow("SELECT * FROM artists_balance WHERE artists_balance.artist_id = ?", artistId)
	err := row.Scan(&existingArtist)
	if err == sql.ErrNoRows {
		return false
	}

	return true
}

func (repo *DBArtistsRepository) CreateOrUpdateArtist(artistId int64, amount int64) {
	if repo.ArtistExists(artistId) {
		repo.AddArtistBalance(artistId, amount)
	} else {
		repo.CreateArtist(artistId, amount)
	}
}

func (repo *DBArtistsRepository) CreateArtist(artistId int64, amount int64) {
	_, err := repo.DB.Execute("INSERT INTO artists_balance (artist_id, balance) VALUES (?,?)", artistId, amount)
	if err != nil {
		fmt.Println("Error creating artists_balance ", err.Error())
	}
}

func (repo *DBArtistsRepository) AddArtistBalance(artistId int64, amount int64) {
	_, err := repo.DB.Execute("UPDATE artists_balance SET balance=balance+? WHERE artists_balance.artist_id = ?", amount, artistId)
	if err != nil {
		fmt.Println("Error updating artists_balance ", err.Error())
	}
}
