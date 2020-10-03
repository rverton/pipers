package main

import "testing"

func TestUnflat(t *testing.T) {

	m := make(map[string]interface{})

	m["foo.bar"] = map[string]interface{}{"barfoo": "123"}

	m = unflat(m)

	if _, ok := m["foo"]; !ok {
		t.Error("could not find `foo`")
	}

}
