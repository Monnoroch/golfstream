package errors

import (
	"fmt"
	"testing"

	// TODO: move to original repo.
	"github.com/Monnoroch/testify/assert"
)

// Test that AddAll() is the same as Add() in a loop.
func TestListAddAll(t *testing.T) {
	examples := [][]error{
		{nil},
		{New("")},
		{New(""), New("1"), New("abcdef")},
		{New(""), nil, New("1"), New("abcdef")},
	}
	for _, el := range examples {
		addLoop := List()
		for _, e := range el {
			addLoop.Add(e)
		}

		assert.Equal(t, addLoop, List().AddAll(el))
	}
}

// Test that Add()-ing a list is the same as AddAll() for errors in that list.
func TestListAddList(t *testing.T) {
	examples := [][]error{
		{nil},
		{New("")},
		{New(""), New("1"), New("abcdef")},
		{New(""), nil, New("1"), New("abcdef")},
	}
	for _, el := range examples {
		assert.Equal(t, List().AddAll(el), List().Add(AsList(el...)))
	}
}

// Test that AsList() is the same as List().AddAll().
func TestAsList(t *testing.T) {
	examples := [][]error{
		{nil},
		{New("")},
		{New(""), New("1"), New("abcdef")},
		{New(""), nil, New("1"), New("abcdef")},
	}
	for _, el := range examples {
		assert.Equal(t, List().AddAll(el), AsList(el...))
	}
}

// Test that empty list returns nil as Err().
func TestListEmpty(t *testing.T) {
	assert.Nil(t, List().Err())
}

// Test that list with one element returns it as Err().
func TestListSingle(t *testing.T) {
	examples := []error{New("1"), New("2"), New("3")}
	for _, e := range examples {
		assert.Equal(t, e, AsList(e).Err())
	}
}

// Test that adding a nil doesn't change the result of Err().
func TestListAddNil(t *testing.T) {
	examples := []*ErrorList{
		AsList(),
		AsList(New("")),
		AsList(New(""), New("1"), New("abcdef")),
	}
	for _, el := range examples {
		assert.Equal(t, el.Err(), el.copy().Add(nil).Err())
	}
}

// Test that adding a nil array doesn't change the result of Err().
func TestListAddAllNil(t *testing.T) {
	examples := []*ErrorList{
		AsList(),
		AsList(New("")),
		AsList(New(""), New("1"), New("abcdef")),
	}
	for _, el := range examples {
		assert.Equal(t, el.Err(), el.copy().AddAll(nil).Err())
	}
}

// Test that adding an empty array doesn't change the result of Err().
func TestListAddAllEmpty(t *testing.T) {
	examples := []*ErrorList{
		AsList(),
		AsList(New("")),
		AsList(New(""), New("1"), New("abcdef")),
	}
	for _, el := range examples {
		assert.Equal(t, el.Err(), el.copy().AddAll([]error{}).Err())
	}
}

// Test that after adding several errors Err() returns an array of them.
func TestListAddErrs(t *testing.T) {
	examples := []struct {
		List *ErrorList
		Errs []error
	}{
		{
			List: AsList(),
			Errs: []error{New("1"), New("2"), New("3")},
		},
		{
			List: AsList(New("")),
			Errs: []error{New(""), New("1"), New("2"), New("3")},
		},
		{
			List: AsList(New(""), New("1"), New("abcdef")),
			Errs: []error{New(""), New("1"), New("abcdef"), New("1"), New("2"), New("3")},
		},
	}
	errs := []error{New("1"), New("2"), New("3")}
	for _, el := range examples {
		assert.Equal(t, AsList(el.Errs...), el.List.copy().AddAll(errs).Err())
	}
}

// Test error message.
func TestListError(t *testing.T) {
	examples := []struct {
		List *ErrorList
		Msg  string
	}{
		{
			List: AsList(),
			Msg:  fmt.Sprintf("%v", nil),
		},
		{
			List: AsList(New("")),
			Msg:  fmt.Sprintf("%v", ""),
		},
		{
			List: AsList(New(""), New("1")),
			Msg:  fmt.Sprintf("%v", []error{New(""), New("1")}),
		},
	}
	for _, e := range examples {
		assert.Equal(t, e.Msg, e.List.Error())
	}
}
