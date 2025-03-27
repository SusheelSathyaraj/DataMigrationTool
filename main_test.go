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
