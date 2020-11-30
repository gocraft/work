package work

import "testing"

func TestMakeIdentifier(t *testing.T) {
	t.Parallel()

	id := makeIdentifier()
	if len(id) < 10 {
		t.Errorf("expected a string of length 10 at least")
	}
}
