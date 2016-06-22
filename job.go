package work

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

type Job struct {
	// Inputs when makin a new job
	Name       string                 `json:"name,omitempty"`
	ID         string                 `json:"id"`
	EnqueuedAt int64                  `json:"t"`
	Args       map[string]interface{} `json:"args"`

	// Inputs when retrying
	Fails    int64  `json:"fails,omitempty"` // number of times this job has failed
	LastErr  string `json:"err,omitempty"`
	FailedAt int64  `json:"failed_at,omitempty"`

	rawJSON      []byte
	dequeuedFrom []byte
	inProgQueue  []byte
	argError     error
}

type Q map[string]interface{}

func newJob(rawJSON, dequeuedFrom, inProgQueue []byte) (*Job, error) {
	var job Job
	err := json.Unmarshal(rawJSON, &job)
	if err != nil {
		return nil, err
	}
	job.rawJSON = rawJSON
	job.dequeuedFrom = dequeuedFrom
	job.inProgQueue = inProgQueue
	return &job, nil
}

func (j *Job) Serialize() ([]byte, error) {
	return json.Marshal(j)
}

func (j *Job) SetArg(key string, val interface{}) {
	if j.Args == nil {
		j.Args = make(map[string]interface{})
	}
	j.Args[key] = val
}

func (j *Job) failed(err error) {
	j.Fails++ // todo: factor into job.failed(runErr)
	j.LastErr = err.Error()
	j.FailedAt = nowEpochSeconds()
}

// ArgString returns j.Args[key] typed to a string. If the key is missing or of the wrong type, it sets an argument error
// on the job. This function is meant to be used in the body of a job handling function while extracting arguments,
// followed by a single call to j.ArgError().
func (j *Job) ArgString(key string) string {
	v, ok := j.Args[key]
	if ok {
		typedV, ok := v.(string)
		if ok {
			return typedV
		} else {
			j.argError = typecastError("string", key, v)
		}
	} else {
		j.argError = missingKeyError("string", key)
	}
	return ""
}

func (j *Job) ArgInt64(key string) int64 {
	v, ok := j.Args[key]
	if ok {
		rVal := reflect.ValueOf(v)
		if isIntKind(rVal) {
			return rVal.Int()
		} else if isUintKind(rVal) {
			vUint := rVal.Uint()
			if vUint <= math.MaxInt64 {
				return int64(vUint)
			}
		} else if isFloatKind(rVal) {
			vFloat64 := rVal.Float()
			vInt64 := int64(vFloat64)
			if vFloat64 == math.Trunc(vFloat64) && vInt64 <= 9007199254740892 && vInt64 >= -9007199254740892 {
				return vInt64
			}
		}
		j.argError = typecastError("int64", key, v)
	} else {
		j.argError = missingKeyError("int64", key)
	}
	return 0
}

func (j *Job) ArgFloat64(key string) float64 {
	v, ok := j.Args[key]
	if ok {
		rVal := reflect.ValueOf(v)
		if isIntKind(rVal) {
			return float64(rVal.Int())
		} else if isUintKind(rVal) {
			return float64(rVal.Uint())
		} else if isFloatKind(rVal) {
			return rVal.Float()
		}
		j.argError = typecastError("float64", key, v)
	} else {
		j.argError = missingKeyError("float64", key)
	}
	return 0.0
}

func (j *Job) ArgBool(key string) bool {
	v, ok := j.Args[key]
	if ok {
		typedV, ok := v.(bool)
		if ok {
			return typedV
		} else {
			j.argError = typecastError("bool", key, v)
		}
	} else {
		j.argError = missingKeyError("bool", key)
	}
	return false
}

func (j *Job) ArgError() error {
	return j.argError
}

func isIntKind(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64
}

func isUintKind(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Uint || k == reflect.Uint8 || k == reflect.Uint16 || k == reflect.Uint32 || k == reflect.Uint64
}

func isFloatKind(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Float32 || k == reflect.Float64
}

func missingKeyError(jsonType, key string) error {
	return fmt.Errorf("looking for a %s in job.Arg[%s] but key wasn't found", jsonType, key)
}

func typecastError(jsonType, key string, v interface{}) error {
	actualType := reflect.TypeOf(v)
	return fmt.Errorf("looking for a %s in job.Arg[%s] but value wasn't right type: %v(%v)", jsonType, key, actualType, v)
}
