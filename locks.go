package main

import (
	"net/http"

	"github.com/gorilla/mux"
)

func (a *App) addLocks(r *mux.Router) {
	r.HandleFunc("/locks", basicAuth(a.addLockHandler)).Methods("POST")
}

func (a *App) addLockHandler(w http.ResponseWriter, r *http.Request) {
}
