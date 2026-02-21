package cppcompat

import (
	"strconv"
	"strings"
)

type fixPair struct {
	Tag   int
	Value string
}

// FIX provides a mutable FIX field container with iterator access.
type FIX struct {
	pairs []fixPair
}

// NewFIX creates an empty FIX container.
func NewFIX() *FIX {
	return &FIX{pairs: []fixPair{}}
}

// Parse parses a FIX message string into this container.
func (fix *FIX) Parse(message string, fieldSep ...rune) *FIX {
	if fix == nil {
		return nil
	}
	separator := '\u0001'
	if len(fieldSep) > 0 {
		separator = fieldSep[0]
	}
	fix.pairs = fix.pairs[:0]
	for _, part := range strings.Split(message, string(separator)) {
		if part == "" {
			continue
		}
		tagValue := strings.SplitN(part, "=", 2)
		if len(tagValue) != 2 {
			continue
		}
		tag, err := strconv.Atoi(tagValue[0])
		if err != nil {
			continue
		}
		fix.pairs = append(fix.pairs, fixPair{Tag: tag, Value: tagValue[1]})
	}
	return fix
}

// Set sets tag value in FIX container.
func (fix *FIX) Set(tag int, value string) *FIX {
	if fix == nil {
		return nil
	}
	for index := range fix.pairs {
		if fix.pairs[index].Tag == tag {
			fix.pairs[index].Value = value
			return fix
		}
	}
	fix.pairs = append(fix.pairs, fixPair{Tag: tag, Value: value})
	return fix
}

// Get returns value for tag.
func (fix *FIX) Get(tag int) (string, bool) {
	if fix == nil {
		return "", false
	}
	for _, pair := range fix.pairs {
		if pair.Tag == tag {
			return pair.Value, true
		}
	}
	return "", false
}

// Data serializes FIX container.
func (fix *FIX) Data(fieldSep ...rune) string {
	if fix == nil {
		return ""
	}
	separator := '\u0001'
	if len(fieldSep) > 0 {
		separator = fieldSep[0]
	}
	parts := make([]string, 0, len(fix.pairs))
	for _, pair := range fix.pairs {
		parts = append(parts, strconv.Itoa(pair.Tag)+"="+pair.Value)
	}
	return strings.Join(parts, string(separator)) + string(separator)
}

// Size returns number of tags.
func (fix *FIX) Size() int {
	if fix == nil {
		return 0
	}
	return len(fix.pairs)
}

// FIXIterator iterates over FIX pairs.
type FIXIterator struct {
	pairs []fixPair
	index int
}

// Begin returns iterator at first pair.
func (fix *FIX) Begin() *FIXIterator {
	if fix == nil {
		return &FIXIterator{pairs: nil, index: 0}
	}
	copied := append([]fixPair(nil), fix.pairs...)
	return &FIXIterator{pairs: copied, index: 0}
}

// End returns iterator at end position.
func (fix *FIX) End() *FIXIterator {
	if fix == nil {
		return &FIXIterator{pairs: nil, index: 0}
	}
	copied := append([]fixPair(nil), fix.pairs...)
	return &FIXIterator{pairs: copied, index: len(copied)}
}

// Next returns next tag/value pair.
func (iterator *FIXIterator) Next() (int, string, bool) {
	if iterator == nil || iterator.index >= len(iterator.pairs) {
		return 0, "", false
	}
	pair := iterator.pairs[iterator.index]
	iterator.index++
	return pair.Tag, pair.Value, true
}
