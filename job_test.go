package work

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type FakeJobPayload struct {
	Str1 string `json:"str1"`
	Int1 int    `json:"int1"`
}

func TestJobPayload(t *testing.T) {
	j := new(Job)
	assert.Nil(t, j.SetPayload(&FakeJobPayload{
		Str1: "foo",
		Int1: 2,
	}))

	payload := new(FakeJobPayload)
	assert.Nil(t, j.UnmarshalPayload(payload))
	assert.Equal(t, "foo", payload.Str1)
	assert.Equal(t, 2, payload.Int1)
}
