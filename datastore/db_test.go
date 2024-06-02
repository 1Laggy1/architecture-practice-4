package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	segmentCount := len(db.segments)
	currentSegment := db.currentSegment.path

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
		}

		newSegmentCount := len(db.segments)
		if newSegmentCount != segmentCount {
			t.Errorf("Unexpected number of segments: expected %d, got %d", segmentCount, newSegmentCount)
		}

		outFile, err := os.Open(currentSegment)
		if err != nil {
			t.Fatal(err)
		}
		defer outFile.Close()

		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		size1 := outInfo.Size()

		if size1 == 0 {
			t.Errorf("Unexpected size: expected > 0, got %d", size1)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("segment rotation", func(t *testing.T) {
		// Write enough data to trigger a segment rotation
		keyPrefix := "key"
		value := make([]byte, maxSegmentSize/2)
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("%s%d", keyPrefix, i)
			err := db.Put(key, string(value))
			if err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}

		// Verify that a new segment was created
		newSegmentCount := len(db.segments)
		if newSegmentCount <= segmentCount {
			t.Errorf("Expected segment count to increase, got %d", newSegmentCount)
		}
	})

	t.Run("find key in older segment", func(t *testing.T) {
		// Add a key to force a segment rotation
		rotationKey := "rotationKey"
		rotationValue := "rotationValue"
		err := db.Put(rotationKey, rotationValue)
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", rotationKey, err)
		}

		// Write enough data to trigger a segment rotation
		keyPrefix := "rotate"
		value := make([]byte, maxSegmentSize/2)
		for i := 0; i < 2; i++ {
			key := fmt.Sprintf("%s%d", keyPrefix, i)
			err := db.Put(key, string(value))
			if err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}

		// Verify that the key in the older segment can still be found
		foundValue, err := db.Get(rotationKey)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", rotationKey, err)
		}
		if foundValue != rotationValue {
			t.Errorf("Unexpected value for key %s: expected %s, got %s", rotationKey, rotationValue, foundValue)
		}
	})
}
