// Copyright 2015 Alex Browne.  All rights reserved.
// Use of this source code is governed by the MIT
// license, which can be found in the LICENSE file.

package jobs

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
)

// decode decodes a slice of bytes and scans the value into dest using the gob package.
// All types are supported except recursive data structures and functions.
func decode(reply []byte, dest interface{}) error {
	// Check the type of dest and make sure it is a pointer to something,
	// otherwise we can't set its value in any meaningful way.
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("jobs: Argument to decode must be pointer. Got %T", dest)
	}

	// Use the gob package to decode the reply and write the result into
	// dest.
	buf := bytes.NewBuffer(reply)
	dec := gob.NewDecoder(buf)
	if err := dec.DecodeValue(val.Elem()); err != nil {
		return err
	}
	return nil
}

// encode encodes data into a slice of bytes using the gob package.
// All types are supported except recursive data structures and functions.
func encode(data interface{}) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	buf := bytes.NewBuffer([]byte{})
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
