package work

import (
	"crypto/rand"
	"fmt"
	"io"
)

// TODO: we probably don't need these strong identifiers, it might be slow to generate
func makeIdentifier() string {
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}
