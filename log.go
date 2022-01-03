package work

import (
	"github.com/juju/errors"
	commonlog "github.com/wallester/common/log"
)

func logError(key string, err error) {
	commonlog.Error(errors.Annotate(err, key))
}
