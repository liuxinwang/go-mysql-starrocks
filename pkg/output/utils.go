package output

import "fmt"

var DeleteColumn = "_delete_sign_"
var DeleteCondition = fmt.Sprintf("%s=1", DeleteColumn)
