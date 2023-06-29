package output

var DeleteColumn = "_delete_sign_"

type Table struct {
	Schema  string
	Name    string
	Columns []string
}

type TableColumn struct {
	Name string
}
