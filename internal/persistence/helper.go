package persistence

import (
	"regexp"
	"strings"
)

func ValidateString(str string) error {
	if len(str) == 0 {
		return ErrStreamEmpty
	}
	isValid := regexp.MustCompile(`^[A-Za-z0-9\-\_]+$`).MatchString
	if !isValid(str) {
		return ErrStreamInvalid
	}
	return nil
}

func CheckStringEmpty(name string) bool {
	name = strings.TrimSpace(name)
	return len(name) == 0
}

func ValidateSnapshotSourceAndName(source, name string) error {
	if CheckStringEmpty(source) {
		return ErrSnapshotSourceEmpty
	}

	if CheckStringEmpty(name) {
		return ErrSnapshotNameEmpty
	}
	return nil
}
