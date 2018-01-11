package work

import (
	"strings"
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestNewSpec(t *testing.T) {
	s := NewSpec()
	o, _ := s.Spec()
	assert.Equal(t, o, "* * * * * *")

	s.Daily("01:23:59")

	o, _ = s.Spec()

	assert.Equal(t, o, "59 23 1 * * *")
}

func TestInterval(t *testing.T) {
	s := NewSpec()

	s.EverySeconds(3)
	o, _ := s.Spec()
	assert.Equal(t, o, "*/3 * * * * *")

	s.EveryMinutes(4)
	o, _ = s.Spec()
	assert.Equal(t, strings.Split(o, " ")[1], "*/4")

	s.EveryHours(5)
	o, _ = s.Spec()
	assert.Equal(t, strings.Split(o, " ")[2], "*/5")
}

func TestBasePeriodical(t *testing.T) {
	s := NewSpec()

	s.Daily("11:05:32")
	o, _ := s.Spec()
	assert.Equal(t, o, "32 5 11 * * *")
}

func TestRaw(t *testing.T) {
	s := NewSpec()
	s.RawCron("1 1 2/3 * * *")
	o, _ := s.Spec()
	assert.Equal(t, o, "1 1 2/3 * * *")

}
