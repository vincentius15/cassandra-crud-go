package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gocql/gocql"

	"github.com/gorilla/mux"
)

type app struct {
	Router *mux.Router
	DB     *gocql.Session
}

func (a *app) getAllAppsEndpoint(w http.ResponseWriter, r *http.Request) {
	apps := PlayStoreApp{}
	result, err := apps.getAll(a.DB)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, result)
}

func (a *app) findAppEndpoint(w http.ResponseWriter, r *http.Request) {
	apps := PlayStoreApp{}
	if err := json.NewDecoder(r.Body).Decode(&apps); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	result, err := apps.find(a.DB)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, result)
}

func (a *app) createAppEndpoint(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	playStoreApp := PlayStoreApp{}
	if err := json.NewDecoder(r.Body).Decode(&playStoreApp); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	if err := playStoreApp.insert(a.DB); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondWithJSON(w, http.StatusCreated, playStoreApp)
}

func (a *app) updateAppEndpoint(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	playStoreApp := PlayStoreApp{}
	if err := json.NewDecoder(r.Body).Decode(&playStoreApp); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if err := playStoreApp.update(a.DB); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]string{"result": "success"})
}

func (a *app) deleteAppEndpoint(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	playStoreApp := PlayStoreApp{}
	if err := json.NewDecoder(r.Body).Decode(&playStoreApp); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if err := playStoreApp.delete(a.DB); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondWithJSON(w, http.StatusOK, map[string]string{"result": "success"})
}

func (a *app) initialize() {
	a.Router = mux.NewRouter()
	a.Router.HandleFunc("/apps", a.getAllAppsEndpoint).Methods("GET")
	a.Router.HandleFunc("/app", a.findAppEndpoint).Methods("POST")
	a.Router.HandleFunc("/app/create", a.createAppEndpoint).Methods("POST")
	a.Router.HandleFunc("/app/update", a.updateAppEndpoint).Methods("PUT")
	a.Router.HandleFunc("/app/delete", a.deleteAppEndpoint).Methods("DELETE")

	cluster := gocql.NewCluster("192.168.33.200")
	cluster.Keyspace = "googleplaystore"
	var err error
	a.DB, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	fmt.Println("cassandra init done")

	if err := http.ListenAndServe(":3000", a.Router); err != nil {
		log.Fatal(err)
	}
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
