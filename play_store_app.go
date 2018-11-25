package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

type PlayStoreApp struct {
	ID            int    `json:"id"`
	App           string `json:"app"`
	Category      string `json:"category"`
	Rating        string `json:"rating"`
	Reviews       string `json:"reviews"`
	Size          string `json:"size"`
	Installs      string `json:"installs"`
	AppType       string `json:"type"`
	Price         string `json:"price"`
	ContentRating string `json:"content_rating"`
	Genres        string `json:"genres"`
	LastUpdated   string `json:"last_updated"`
	CurrentVer    string `json:"current_ver"`
	AndroidVer    string `json:"android_ver"`
	AndroidVer2   string `json:"android_ver2"`
}

func (p *PlayStoreApp) getAll(DB *gocql.Session) ([]PlayStoreApp, error) {
	fmt.Println("Getting all apps")
	iter := DB.Query("SELECT * FROM apps").Iter()
	return toStruct(iter), nil
}

func (p *PlayStoreApp) find(DB *gocql.Session) ([]PlayStoreApp, error) {
	fmt.Printf("Find app with name = %s\n", p.App)
	iter := DB.Query("SELECT * FROM apps WHERE app = ? allow filtering", p.App).Iter()
	return toStruct(iter), nil
}

func (p *PlayStoreApp) insert(DB *gocql.Session) error {
	fmt.Printf("Creating new app with name = %s\n", p.App)
	if err := DB.Query("INSERT INTO apps(id, app, category, rating, reviews, size, installs, type, price, content_rating, genres, last_updated, current_ver, android_ver, android_ver2) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		p.ID, p.App, p.Category, p.Rating, p.Reviews, p.Size, p.Installs, p.AppType, p.Price, p.ContentRating, p.Genres, p.LastUpdated, p.CurrentVer, p.AndroidVer, p.AndroidVer2).Exec(); err != nil {
		fmt.Println("Error while inserting app")
		fmt.Println(err)
		return err
	}
	return nil
}

func (p *PlayStoreApp) update(DB *gocql.Session) error {
	fmt.Printf("Updating app with id = %d\n", p.ID)
	if err := DB.Query("UPDATE apps SET app = ?, category = ?, rating = ?, reviews = ?, size = ?, installs = ?, type = ?, price = ?, content_rating = ?, genres = ?, last_updated = ?, current_ver = ?, android_ver = ?, android_ver2 = ? WHERE id = ?",
		p.App, p.Category, p.Rating, p.Reviews, p.Size, p.Installs, p.AppType, p.Price, p.ContentRating, p.Genres, p.LastUpdated, p.CurrentVer, p.AndroidVer, p.AndroidVer2, p.ID).Exec(); err != nil {
		fmt.Println("Error while updating app")
		fmt.Println(err)
		return err
	}
	return nil
}

func (p *PlayStoreApp) delete(DB *gocql.Session) error {
	fmt.Printf("Deleting app with id = %d\n", p.ID)
	if err := DB.Query("DELETE FROM apps WHERE id = ?", p.ID).Exec(); err != nil {
		fmt.Println("Error while deleting app")
		fmt.Println(err)
		return err
	}
	return nil
}

func toStruct(iter *gocql.Iter) []PlayStoreApp {
	var apps []PlayStoreApp
	m := map[string]interface{}{}
	for iter.MapScan(m) {
		apps = append(apps, PlayStoreApp{
			ID:            m["id"].(int),
			App:           m["app"].(string),
			Category:      m["category"].(string),
			Rating:        m["rating"].(string),
			Reviews:       m["reviews"].(string),
			Size:          m["size"].(string),
			Installs:      m["installs"].(string),
			AppType:       m["type"].(string),
			Price:         m["price"].(string),
			ContentRating: m["content_rating"].(string),
			Genres:        m["genres"].(string),
			LastUpdated:   m["last_updated"].(string),
			CurrentVer:    m["current_ver"].(string),
			AndroidVer:    m["android_ver"].(string),
			AndroidVer2:   m["android_ver2"].(string),
		})
		m = map[string]interface{}{}
	}
	return apps
}
