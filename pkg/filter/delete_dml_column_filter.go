package filter

type deleteDmlColumnFilter struct {
	BinlogFilter
	columns []string
}

func (ddcf *deleteDmlColumnFilter) Filter() {

}
