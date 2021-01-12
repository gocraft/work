package work

import (
	"github.com/google/uuid"
)

func makeIdentifier() string {
	return uuid.New().String()
}
