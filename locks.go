package main

import (
	"net/http"
	"strconv"

	"encoding/json"
	"fmt"

	"github.com/gorilla/context"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
)

type CreateLockRequest struct {
	Path string `json:path`
}

type ListLocksRequest struct {
	Path   string `schema:path`
	ID     string `schema:id`
	Cursor string `schema:cursor`
	Limit  int    `schema:limit`
}

type VerifyLocksRequest struct {
	Cursor string `json:cursor`
	Limit  int    `json:limit`
}

type DeleteLockRequest struct {
	Force bool `json:force`
}

func jsonString(s string) []byte {
	os, err := json.Marshal(s)
	if err != nil {
		return []byte{}
	}
	return os
}

func writeLocksResponse(w http.ResponseWriter, r *http.Request, status int, message string) {
	w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
	w.WriteHeader(status)
	fmt.Fprint(w, message)
	logRequest(r, status)
}

func buildLockInfoResponse(lock *MetaLock) string {
	s := fmt.Sprintf(`{"id":"%x","path":%s,"locked_at":%s,"owner":{"name":%s}}`,
		lock.ID, jsonString(lock.Path), jsonString(lock.LockedAt), jsonString(lock.Owner))
	return s
}

func writeLocksErrorResponseExtra(w http.ResponseWriter, r *http.Request, status int, msg string, extra string) {
	requestID := context.Get(r, "RequestID").(string)
	if extra != "" {
		extra = "," + extra
	}
	if msg == "" {
		msg = http.StatusText(status)
	}
	writeLocksResponse(w, r, status, fmt.Sprintf(`{"message":%s,"request_id":%s%s}`,
		jsonString(msg), jsonString(requestID), extra))
}

func writeLocksErrorResponse(w http.ResponseWriter, r *http.Request, status int) {
	writeLocksErrorResponseExtra(w, r, status, "", "")
}

func (a *App) checkAuthentication(w http.ResponseWriter, r *http.Request) (string, error) {

	user, pass, ok := r.BasicAuth()

	ret := checkBasicAuth(user, pass, ok)
	if !ret {
		ret = a.metaStore.ValidateUser(user, pass)
	}
	if !ret {
		writeLocksErrorResponse(w, r, 403)
		return "", errUnauthorized
	}
	return user, nil
}

func (a *App) addLocks(r *mux.Router) {
	r.HandleFunc("/{user}/{repo}/locks", a.addLockHandler).Methods("POST")
	r.HandleFunc("/{user}/{repo}/locks", a.listLocksHandler).Methods("GET")
	r.HandleFunc("/{user}/{repo}/locks/verify", a.verifyLocksHandler).Methods("POST")
	r.HandleFunc("/{user}/{repo}/locks/{id}/unlock", a.deleteLockHandler).Methods("POST")
}

// responds with 201, 409 or 403
func (a *App) addLockHandler(w http.ResponseWriter, r *http.Request) {
	user, err := a.checkAuthentication(w, r)
	if err != nil {
		return
	}
	dec := json.NewDecoder(r.Body)
	var params CreateLockRequest
	err = dec.Decode(&params)
	if err != nil {
		writeLocksErrorResponse(w, r, 400)
		return
	}
	lock, err := a.metaStore.LockAdd(params.Path, user)
	if err == errDuplicateObject {
		// Not specified in the API: what to do if lock
		// already exists for this same user. Grant or error?
		// Default to error for now
		msg := `"lock":` + buildLockInfoResponse(lock)
		writeLocksErrorResponseExtra(w, r, 409, "", msg)
		return
	} else if err != nil {
		writeLocksErrorResponseExtra(w, r, 500, err.Error(), "")
		return
	}
	msg := `{"lock":` + buildLockInfoResponse(lock) + `}`
	writeLocksResponse(w, r, 201, msg)
}

func buildLocksListJSON(locks []MetaLock) string {
	sep := ""
	msg := `[`
	for _, l := range locks {
		msg = msg + buildLockInfoResponse(&l) + sep
		sep = ","
	}
	return msg + "]"
}

// responds with 201, 403 or 500
func (a *App) listLocksHandler(w http.ResponseWriter, r *http.Request) {
	_, err := a.checkAuthentication(w, r)
	if err != nil {
		return
	}
	var params ListLocksRequest
	r.ParseForm()
	if err = schema.NewDecoder().Decode(&params, r.Form); err != nil {
		writeLocksErrorResponseExtra(w, r, 400, err.Error(), "")
		return
	}
	var locks []MetaLock
	var cursor uint64
	var pending bool
	if params.ID != "" {
		id, err := strconv.ParseUint(params.ID, 16, 64)
		if err == nil {
			meta, err := a.metaStore.LockGet(id)
			if err != nil {
				locks = append(locks, *meta)
			}
		}
	} else {
		cursor, err = strconv.ParseUint(params.Cursor, 16, 64)
		if err != nil {
			cursor = 0
		}
		locks, cursor, pending, err = a.metaStore.LockList(params.Path, cursor, params.Limit)
	}
	if err != nil {
		writeLocksErrorResponseExtra(w, r, 500, err.Error(), "")
		return
	}
	msg := `{"locks": ` + buildLocksListJSON(locks)
	if pending {
		msg = msg + fmt.Sprintf(`,"next_cursor":"%x"`, cursor)
	}
	msg = msg + "}"
	writeLocksResponse(w, r, 200, msg)
}

// responds with 201, 404, 403 or 500
func (a *App) verifyLocksHandler(w http.ResponseWriter, r *http.Request) {
	user, err := a.checkAuthentication(w, r)
	if err != nil {
		return
	}
	dec := json.NewDecoder(r.Body)
	var params VerifyLocksRequest
	err = dec.Decode(&params)
	if err != nil {
		writeLocksErrorResponse(w, r, 400)
		return
	}
	var locks []MetaLock
	var pending bool
	cursor, err := strconv.ParseUint(params.Cursor, 16, 64)
	if err != nil {
		cursor = 0
	}
	locks, cursor, pending, err = a.metaStore.LockList("", cursor, params.Limit)
	if err != nil {
		writeLocksErrorResponseExtra(w, r, 500, err.Error(), "")
		return
	}
	var ours, theirs []MetaLock
	for _, l := range locks {
		if l.Owner == user {
			ours = append(ours, l)
		} else {
			theirs = append(theirs, l)
		}
	}
	msg := `{"ours": ` + buildLocksListJSON(ours) + `,"theirs":` + buildLocksListJSON(theirs)
	if pending {
		msg = msg + fmt.Sprintf(`,"next_cursor":"%x"`, cursor)
	}
	msg = msg + "}"
	writeLocksResponse(w, r, 200, msg)
}

// responds with 201, 403 or 500
func (a *App) deleteLockHandler(w http.ResponseWriter, r *http.Request) {
	user, err := a.checkAuthentication(w, r)
	if err != nil {
		return
	}
	vars := mux.Vars(r)
	id, err := strconv.ParseUint(vars["id"], 16, 64)
	if err != nil {
		writeLocksErrorResponse(w, r, 400)
		return
	}
	dec := json.NewDecoder(r.Body)
	var params DeleteLockRequest
	err = dec.Decode(&params)
	if err != nil {
		writeLocksErrorResponse(w, r, 400)
		return
	}
	lock, err := a.metaStore.LockDelete(id, user, params.Force)
	if err == errUnauthorized {
		writeLocksErrorResponse(w, r, 403)
		return
	} else if err != nil {
		writeLocksErrorResponseExtra(w, r, 500, err.Error(), "")
		return
	}
	msg := `{"lock":` + buildLockInfoResponse(lock) + `}`
	writeLocksResponse(w, r, 200, msg)
}
