package output

type Table struct {
	Schema  string
	Name    string
	Columns []string
}

type TableColumn struct {
	Name string
}

func (t *Table) GetTable(db string, table string) {

}
