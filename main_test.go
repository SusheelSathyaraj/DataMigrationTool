package main

import (
	"testing"
)

// tests validateInput function
func TestValidateInput(t *testing.T) {
	tests := []struct {
		source string
		target string
		mode   string
		expect bool
	}{
		{"", "", "", false},
		{"", "", "full", false},
		{"mysql", "postgresql", "full", true},
		{"mysql", "postgresql", "", false},
		{"mysql", "mongodb", "incremental", true},
		{"MYSQL", "postgresql", "", false},
		{"MYSQL", "MONGODB", "SCHEDULED", true},
		{"mysql", "postgresql", "FULL", true},
		{"mysql", "", "full", false},
		{"", "MONGODB", "", false},
		{"MySQL", "MongoDb", "Full", true},
	}

	for i, tc := range tests {
		err := validateInput(tc.source, tc.target, tc.mode)
		if (err == nil) != tc.expect {
			t.Errorf("[Test case: %d]validateInput func(%s,%s,%s) expected success: %v, got error:%v", i+1, tc.source, tc.target, tc.mode, tc.expect, err)
		}
	}
}

// tests isValidDatabase function
func TestIsValidDatabase(t *testing.T) {
	tests := []struct {
		db     string
		slice  []string
		expect bool
	}{
		{"mysql", []string{"mysql", "postgresql", "mongodb"}, true},
		{"POSTGRESQL", []string{"mysql", "postgresql", "mongodb"}, true},
		{"MongoDb", []string{"mysql", "postgresql", "mongodb"}, true},
		{"mysq", []string{"mysql", "postgresql", "mongodb"}, false},
		{"", []string{"mysql", "postgresql", "mongodb"}, false},
		{"", []string{"mysql", "postgresql"}, false},
		{"", []string{}, false},
	}

	for i, tc := range tests {
		err := isValidDatabase(tc.db, tc.slice)
		if (err) != tc.expect {
			t.Errorf("Test case: %d, for isValidDatabase(%s,%v) has %v, Error: %v", i+1, tc.db, tc.slice, tc.expect, err)
		}
	}
}
