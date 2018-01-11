package work

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type fieldSpec struct {
	start    int
	interval int
}

// TimerSpec schedule timer
type TimerSpec struct {
	fields   []*fieldSpec
	err      error
	interval int
	rawCron  string
}

// nolint
const (
	secondIdx = iota //second
	minuteIdx
	hourIdx
	domIdx //day of month
	monthIdx
	dowIdx //day of week
)

// WeekDay enum for week day
type WeekDay int

// Sunday WeekDay enum
const (
	Sunday WeekDay = iota
	Monday
	Tuesday
	Wednesday
	Thursday
	Friday
	Saturday
)

const (
	regTime   = `^(20|21|22|23|[0-1]\d):[0-5]\d:[0-5]\d$`
	regHour   = `^[0-5]\d:[0-5]\d$`
	regMinute = `^[0-5]\d$`
)

// NewSpec Create a new spec
func NewSpec() *TimerSpec {
	t := &TimerSpec{
		fields: make([]*fieldSpec, 6),
	}
	return t.init()
}

func (t *TimerSpec) init() *TimerSpec {
	for k := range t.fields {
		t.fields[k] = &fieldSpec{
			start:    -1, // will format as "*"
			interval: 0,
		}
	}
	t.err = nil
	t.rawCron = ""
	return t
}

//---------common used cron schedule-------------------------------------------

// Minutely execute every minite start at "start", format: "ss"
func (t *TimerSpec) Minutely(start string) {
	r := regexp.MustCompile(regMinute)
	if !r.MatchString(start) {
		t.err = errors.New("invalid minutely time spec, should be ss")
		return
	}
	t.init()
	t.fields[secondIdx].start, _ = strconv.Atoi(start)
}

// Hourly execute every hour start at "start", format: "mm:ss"
func (t *TimerSpec) Hourly(start string) {
	r := regexp.MustCompile(regHour)
	if !r.MatchString(start) {
		t.err = errors.New("invalid hourly time spec, should be mm:ss")
		return
	}
	l := strings.Split(start, ":")
	t.init()
	t.fields[minuteIdx].start, _ = strconv.Atoi(l[0])
	t.fields[secondIdx].start, _ = strconv.Atoi(l[1])
}

// Daily execute every day at "start", format: "hh:mm:ss"
func (t *TimerSpec) Daily(start string) {
	r := regexp.MustCompile(regTime)
	if !r.MatchString(start) {
		t.err = errors.New("invalid time spec, should be hh:mm:ss")
		return
	}
	l := strings.Split(start, ":")
	t.init()
	t.fields[hourIdx].start, _ = strconv.Atoi(l[0])
	t.fields[minuteIdx].start, _ = strconv.Atoi(l[1])
	t.fields[secondIdx].start, _ = strconv.Atoi(l[2])
}

// Weekly execute every week start at "start", run at "weekday"
func (t *TimerSpec) Weekly(start string, day WeekDay) {
	if start == "" {
		start = "00:00:00"
	}
	t.Daily(start)

	t.fields[dowIdx].start = int(day)
}

// Monthly execute every month, start at "start", run at "dayIndex"
func (t *TimerSpec) Monthly(start string, day int) {
	if day < 0 || day > 27 {
		t.err = errors.New("invalid day of month, should be 0~27")
	}
	if start == "" {
		start = "00:00:00"
	}
	t.Daily(start)
	t.fields[domIdx].start = day
}

// Yearly execute every year, not implemented, trust me, you will not use it.

//---------periodic cron schedule-------------------------------------------

// Seconds used as Every(3).Seconds(): execute every 3 seconds
func (t *TimerSpec) EverySeconds(interval int) {
	t.init()
	t.fields[secondIdx].interval = interval
}

// Minutes used as Every(3).Minutes(): execute every 3 minutes
func (t *TimerSpec) EveryMinutes(interval int) {
	t.init()
	t.fields[secondIdx].start = time.Now().Second()
	t.fields[minuteIdx].interval = interval
}

// Hours used as Every(3).Hours(): execute every 3 hours
func (t *TimerSpec) EveryHours(interval int) {
	t.init()
	now := time.Now()
	t.fields[secondIdx].start = now.Second()
	t.fields[minuteIdx].start = now.Minute()
	t.fields[hourIdx].interval = interval
}

//---------manual setting cron schedule-------------------------------------------

//RawCron manually setting cron schedule, format: "s m h dom month dow", be careful using
func (t *TimerSpec) RawCron(c string) {
	t.init()
	t.rawCron = c
}

// Spec encode the spec to cron format
func (t *TimerSpec) Spec() (string, error) {
	if t.err != nil {
		return "", t.err
	}

	if t.rawCron != "" {
		return t.rawCron, nil
	}

	var out string
	for _, f := range t.fields {
		out = out + assembleField(f) + " "
	}
	return strings.TrimSpace(out), nil
}

func assembleField(f *fieldSpec) string {
	var out string
	if f.start < 0 {
		out = "*"
	} else {
		out = strconv.Itoa(f.start)
	}

	if f.interval > 0 {
		out = fmt.Sprintf("%s/%d", out, f.interval)
	}
	return out
}
