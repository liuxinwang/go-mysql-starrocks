package output

import "fmt"

var DeleteColumn = "_delete_sign_"
var DeleteCondition = fmt.Sprintf("%s=1", DeleteColumn)
var RetryCount = 3
var RetryInterval = 5
