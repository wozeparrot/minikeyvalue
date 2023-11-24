package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/util"
)

// *** Master Server ***

type ListResponse struct {
	Next string   `json:"next"`
	Keys []string `json:"keys"`
}

func (a *App) QueryHandler(key []byte, w http.ResponseWriter, r *http.Request) {
	// operation is first query parameter (e.g. ?list&limit=10)
	operation := strings.Split(r.URL.RawQuery, "&")[0]
	switch operation {
	case "list", "writing", "unlinked":
		start := r.URL.Query().Get("start")
		limit := 0
		qlimit := r.URL.Query().Get("limit")
		if qlimit != "" {
			nlimit, err := strconv.Atoi(qlimit)
			if err != nil {
				w.WriteHeader(400)
				return
			}
			limit = nlimit
		}

		slice := util.BytesPrefix(key)
		if start != "" {
			slice.Start = []byte(start)
		}
		iter := a.db.NewIterator(slice, nil)
		defer iter.Release()
		keys := make([]string, 0)
		next := ""
		for iter.Next() {
			rec := toRecord(iter.Value())
			if (rec.deleted != NO && operation == "list") ||
				(rec.deleted != INIT && operation == "writing") ||
				(rec.deleted != SOFT && operation == "unlinked") {
				continue
			}
			if len(keys) > 2147483646 { // too large (need to specify limit)
				w.WriteHeader(413)
				return
			}
			if limit > 0 && len(keys) == limit { // limit results returned
				next = string(iter.Key())
				break
			}
			keys = append(keys, string(iter.Key()))
		}
		str, err := json.Marshal(ListResponse{Next: next, Keys: keys})
		if err != nil {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write(str)
		return
	default:
		w.WriteHeader(403)
		return
	}
}

func (a *App) Delete(key []byte, unlink bool) int {
	// delete the key, first locally
	rec := a.GetRecord(key)
	if rec.deleted == HARD || (unlink && rec.deleted == SOFT) {
		return 404
	}

	if !unlink && a.protect && rec.deleted == NO {
		return 403
	}

	// mark as deleted
	if !a.PutRecord(key, Record{rec.rvolumes, SOFT, rec.hash}) {
		return 500
	}

	if !unlink {
		// then remotely, if this is not an unlink
		var wg sync.WaitGroup
		errs := make(chan error, len(rec.rvolumes)*2)
		path := key2path(key)
		for i, volume := range rec.rvolumes {
			wg.Add(1)
			go func(volume string, i int) {
				defer wg.Done()
				remote := fmt.Sprintf("http://%s%s", volume, path)
				if err := remote_delete(remote); err != nil {
					// if this fails, it's possible to get an orphan file
					// but i'm not really sure what else to do?
					fmt.Printf("replica %d delete failed: %s\n", i, remote)
					errs <- err
				}
			}(volume, i)

		}
		// delete the key from the remotes as well
		for i, volume := range rec.rvolumes {
			wg.Add(1)
			go func(volume string, i int) {
				defer wg.Done()
				remote := fmt.Sprintf("http://%s%s.key", volume, path)
				if err := remote_delete(remote); err != nil {
					fmt.Printf("replica %d delete key failed: %s\n", i, remote)
					errs <- err
				}
			}(volume, i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				return 500
			}
		}

		// this is a hard delete in the database, aka nothing
		a.db.Delete(key, nil)
	}

	// 204, all good
	return 204
}

func (a *App) WriteToReplicas(key []byte, value io.Reader, valuelen int64) int {
	// we don't have the key, compute the remote URL
	kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)

	// push to leveldb as initializing, and without a hash since we don't have it yet
	if !a.PutRecord(key, Record{kvolumes, INIT, ""}) {
		return 500
	}

	// write to each replica
	// because we know that the contents are going to be small, we can just read it all into memory
	// and then write it to each replica concurrently
	body, err := io.ReadAll(value)
	if err != nil {
		fmt.Println("error reading body", err)
		return 500
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(kvolumes)*2)
	path := key2path(key)
	for i, volume := range kvolumes {
		wg.Add(1)
		go func(volume string, i int, body io.Reader) {
			defer wg.Done()
			remote := fmt.Sprintf("http://%s%s", volume, path)
			if err := remote_put(remote, valuelen, body); err != nil {
				fmt.Printf("replica %d write failed: %s\n", i, remote)
				errs <- err
			}
		}(volume, i, bytes.NewReader(body))
	}
	// write the key to the remotes as well
	for i, volume := range kvolumes {
		wg.Add(1)
		go func(volume string, i int) {
			defer wg.Done()
			remote := fmt.Sprintf("http://%s%s.key", volume, path)
			if err := remote_put(remote, int64(len(key)), bytes.NewReader(key)); err != nil {
				fmt.Printf("replica %d write key failed: %s\n", i, remote)
				errs <- err
			}
		}(volume, i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			// try not to leave key in the initializing state
			a.PutRecord(key, Record{kvolumes, SOFT, ""})
			return 500
		}
	}

	var hash = ""
	if a.md5sum {
		// compute the hash of the value
		hash = fmt.Sprintf("%x", md5.Sum(body))
	}

	// push to leveldb as existing
	// note that the key is locked, so nobody wrote to the leveldb
	if !a.PutRecord(key, Record{kvolumes, NO, hash}) {
		return 500
	}

	// 201, all good
	return 201
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.URL.Path)

	log.Println(r.Method, r.URL, r.ContentLength, r.Header["Range"])

	// this is a list query
	if len(r.URL.RawQuery) > 0 && r.Method == "GET" {
		a.QueryHandler(key, w, r)
		return
	}

	// lock the key while an operation that needs the key locked is in progress
	if r.Method == "PUT" || r.Method == "PATCH" || r.Method == "DELETE" || r.Method == "UNLINK" || r.Method == "REBALANCE" {
		if !a.LockKey(key) {
			// Conflict, retry later
			w.WriteHeader(409)
			return
		}
		defer a.UnlockKey(key)
	}

	switch r.Method {
	case "GET", "HEAD":
		rec := a.GetRecord(key)
		var remote string
		if len(rec.hash) != 0 {
			// note that the hash is always of the whole file, not the content requested
			w.Header().Set("Content-Md5", rec.hash)
		}
		if rec.deleted != NO {
            w.Header().Set("Content-Length", "0")
            w.WriteHeader(404)
            return
		} else {
			kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)
			if needs_rebalance(rec.rvolumes, kvolumes) {
				w.Header().Set("Key-Balance", "unbalanced")
				fmt.Println("on wrong volumes, needs rebalance")
			} else {
				w.Header().Set("Key-Balance", "balanced")
			}
			w.Header().Set("Key-Volumes", strings.Join(rec.rvolumes, ","))

			// check the volume servers in a random order
			good := false
			for _, vn := range rand.Perm(len(rec.rvolumes)) {
				remote = fmt.Sprintf("http://%s%s", rec.rvolumes[vn], key2path(key))
				found, _ := remote_head(remote, a.voltimeout)
				if found {
					good = true
					break
				}
			}
			// if not found on any volume servers, fail before the redirect
			if !good {
				w.Header().Set("Content-Length", "0")
				w.WriteHeader(404)
				return
			}
			// note: this can race and fail, but in that case the client will handle the retry
		}
		w.Header().Set("Location", remote)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(302)
	case "PUT", "PATCH":
		// no empty values
		if r.ContentLength == 0 {
			w.WriteHeader(411)
			return
		}

		// check if we already have the key, and it's not deleted (unless this is a PATCH)
		if r.Method == "PUT" {
			rec := a.GetRecord(key)
			if rec.deleted == NO {
				// Forbidden to overwrite with PUT
				w.WriteHeader(403)
				return
			}
		}

		status := a.WriteToReplicas(key, r.Body, r.ContentLength)
		w.WriteHeader(status)
	case "DELETE", "UNLINK":
		status := a.Delete(key, r.Method == "UNLINK")
		w.WriteHeader(status)
	case "REBALANCE":
		rec := a.GetRecord(key)
		if rec.deleted != NO {
			w.WriteHeader(404)
			return
		}

		kvolumes := key2volume(key, a.volumes, a.replicas, a.subvolumes)
		rbreq := RebalanceRequest{key: key, volumes: rec.rvolumes, kvolumes: kvolumes}
		if !rebalance(a, rbreq) {
			w.WriteHeader(400)
			return
		}

		// 204, all good
		w.WriteHeader(204)
	}
}
