package main

import (
	"fmt"
	"os"
	"testing"
)

var (
	metaStoreTest *MetaStore
)

func TestGetWithAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	meta, err := metaStoreTest.Get(&RequestVars{Authorization: testAuth, Oid: contentOid})
	if err != nil {
		t.Fatalf("Error retreiving meta: %s", err)
	}

	if meta.Oid != contentOid {
		t.Errorf("expected to get content oid, got: %s", meta.Oid)
	}

	if meta.Size != contentSize {
		t.Errorf("expected to get content size, got: %d", meta.Size)
	}
}

func TestGetWithoutAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	_, err := metaStoreTest.Get(&RequestVars{Authorization: badAuth, Oid: contentOid})
	if !isAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}
}

func TestPutWithAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	meta, err := metaStoreTest.Put(&RequestVars{Authorization: testAuth, Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected put to succeed, got : %s", err)
	}

	if meta.Existing {
		t.Errorf("expected meta to not have existed")
	}

	meta, err = metaStoreTest.Get(&RequestVars{Authorization: testAuth, Oid: nonexistingOid})
	if err != nil {
		t.Errorf("expected to be able to retreive new put, got : %s", err)
	}

	if meta.Oid != nonexistingOid {
		t.Errorf("expected oids to match, got: %s", meta.Oid)
	}

	if meta.Size != 42 {
		t.Errorf("expected sizes to match, got: %d", meta.Size)
	}

	meta, err = metaStoreTest.Put(&RequestVars{Authorization: testAuth, Oid: nonexistingOid, Size: 42})
	if err != nil {
		t.Errorf("expected put to succeed, got : %s", err)
	}

	if !meta.Existing {
		t.Errorf("expected meta to now exist")
	}
}

func TestPuthWithoutAuth(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	_, err := metaStoreTest.Put(&RequestVars{Authorization: badAuth, Oid: contentOid, Size: 42})
	if !isAuthError(err) {
		t.Errorf("expected auth error, got: %s", err)
	}
}

func TestLocks(t *testing.T) {
	setupMeta()
	defer teardownMeta()

	list, _, pending, err := metaStoreTest.LockList("", 0, 100)
	if err != nil {
		t.Errorf("expected 1) LockList to succeed, got : %s", err)
	}
	if len(list) > 0 {
		t.Errorf("expected 1) list to be empty, got : %d elements", len(list))
	}
	if pending {
		t.Errorf("expected 5) list to not have any items pending")
	}

	lock1, err := metaStoreTest.LockAdd("/test", "owner")
	if err != nil {
		t.Errorf("expected 2) LockAdd to succeed, got : %s", err)
	}
	if lock1.Path != "/test" {
		t.Errorf("expected 2) path to be '/test', got : %s", lock1.Path)
	}
	if lock1.Owner != "owner" {
		t.Errorf("expected 2) owner to be 'owner', got : %s", lock1.Owner)
	}

	lock2, err := metaStoreTest.LockAdd("/test2", "owner")
	if err != nil {
		t.Errorf("expected 3) LockAdd to succeed, got : %s", err)
	}

	lock, err := metaStoreTest.LockAdd("/test", "owner")
	if err == nil {
		t.Errorf("expected 4) LockAdd to fail, got : %s", lock.Path)
	}

	list, _, pending, err = metaStoreTest.LockList("", 0, 100)
	if err != nil {
		t.Errorf("expected 5) LockList to succeed, got : %s", err)
	}
	if len(list) != 2 {
		t.Errorf("expected 5) list to contain 2 elements, got : %d elements", len(list))
	}
	if pending {
		t.Errorf("expected 5) list to not have any items pending")
	}
	if !((list[0].Path == "/test" && list[1].Path == "/test2") ||
		(list[0].Path == "/test2" && list[1].Path == "/test")) {
		t.Errorf("expected 5) list to have two distinct paths, got %s and %s", list[0].Path, list[1].Path)
	}

	list1, cursor, pending, err := metaStoreTest.LockList("", 0, 1)
	if err != nil {
		t.Errorf("expected 6) LockList to succeed, got : %s", err)
	}
	if len(list1) != 1 {
		t.Errorf("expected 6) list to contain 1 element, got : %#v elements", list1)
	}
	if !pending {
		t.Errorf("expected 6) list to have items pending")
	}
	if cursor != 2 {
		t.Errorf("expected 6) cursor to point to 2 but got %d", cursor)
	}

	list2, _, pending, err := metaStoreTest.LockList("", cursor, 1)
	if err != nil {
		t.Errorf("expected 7) LockList to succeed, got : %s", err)
	}
	if len(list2) != 1 {
		t.Errorf("expected 6) list to contain 1 element, got : %#v elements", list2)
	}
	if pending {
		t.Errorf("expected 7) list to not have items pending")
	}

	if !((list1[0].Path == "/test" && list2[0].Path == "/test2") ||
		(list1[0].Path == "/test2" && list2[0].Path == "/test")) {
		t.Errorf("expected 6,7) lists to have two distinct paths, got %s and %s", list1[0].Path, list2[0].Path)
	}

	list, _, pending, err = metaStoreTest.LockList("/test2", 0, 100)
	if err != nil {
		t.Errorf("expected 8) LockList to succeed, got : %s", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 8) list to have 1 elements, got : %d elements", len(list))
	}

	list, _, pending, err = metaStoreTest.LockList("/nothing", 0, 100)
	if err != nil {
		t.Errorf("expected 9) LockList to succeed, got : %s", err)
	}
	if len(list) > 0 {
		t.Errorf("expected 9) list to be empty, got : %d elements", len(list))
	}

	lock, err = metaStoreTest.LockDelete(lock2.ID, "wrong", false)
	if err == nil {
		t.Errorf("expected 10) LockDelete to fail, got : %s", lock.Path)
	}

	lock, err = metaStoreTest.LockDelete(lock2.ID, "owner", false)
	if err != nil {
		t.Errorf("expected 11) LockDelete to succeed, got : %s", err)
	}

	list, _, _, err = metaStoreTest.LockList("/test2", 0, 100)
	if err != nil {
		t.Errorf("expected 12) LockList to succeed, got : %s", err)
	}
	if len(list) > 0 {
		t.Errorf("expected 12) list to be empty, got : %d elements", len(list))
	}

	list, _, _, err = metaStoreTest.LockList("/test", 0, 100)
	if err != nil {
		t.Errorf("expected 13) LockList to succeed, got : %s", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 13) list to have 1 elements, got : %d elements", len(list))
	}

	lock, err = metaStoreTest.LockDelete(lock1.ID, "wrong", true)
	if err != nil {
		t.Errorf("expected 14) LockDelete to succeed, got : %s", err)
	}

	list, _, _, err = metaStoreTest.LockList("", 0, 100)
	if err != nil {
		t.Errorf("expected 15) LockList to succeed, got : %s", err)
	}
	if len(list) > 0 {
		t.Errorf("expected 15) list to be empty, got : %d elements", len(list))
	}

}

func setupMeta() {
	store, err := NewMetaStore("test-meta-store.db")
	if err != nil {
		fmt.Printf("error initializing test meta store: %s\n", err)
		os.Exit(1)
	}

	metaStoreTest = store
	if err := metaStoreTest.AddUser(testUser, testPass); err != nil {
		teardownMeta()
		fmt.Printf("error adding test user to meta store: %s\n", err)
		os.Exit(1)
	}

	rv := &RequestVars{Authorization: testAuth, Oid: contentOid, Size: contentSize}
	if _, err := metaStoreTest.Put(rv); err != nil {
		teardownMeta()
		fmt.Printf("error seeding test meta store: %s\n", err)
		os.Exit(1)
	}
}

func teardownMeta() {
	metaStoreTest.Close()
	os.RemoveAll("test-meta-store.db")
}
