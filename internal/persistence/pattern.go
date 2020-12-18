package persistence

import (
	"regexp"
	"strings"
)

type SearchPattern struct {
	original string
	regStr   string
	regexp   *regexp.Regexp
	valid    bool
}

func Pattern(s string) SearchPattern {
	regStr := strings.ReplaceAll(s, "*", `(\w\d)*`)
	reg, err := regexp.Compile(regStr)
	return SearchPattern{
		original: s,
		regStr:   regStr,
		regexp:   reg,
		valid:    err == nil,
	}
}

func (p SearchPattern) Match(name string) bool {
	if !p.valid {
		return false
	}
	return p.regexp.MatchString(name)
}

func (p SearchPattern) Valid() bool {
	return p.valid
}

func (p SearchPattern) String() string {
	return p.original
}

func (p SearchPattern) RegexpString() string {
	return p.regStr
}
