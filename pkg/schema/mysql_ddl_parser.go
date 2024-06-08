package schema

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"strings"
)

var p *parser.Parser

func init() {
	p = parser.New()
}

func parse(sql string) (*ast.StmtNode, error) {
	stmtNodes, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func columnDefParse(columnDef *ast.ColumnDef) TableColumn {
	tableColumn := TableColumn{}
	tableColumn.Name = columnDef.Name.String()
	tableColumn.RawType = columnDef.Tp.String()
	switch columnDef.Tp.GetType() {
	case mysql.TypeEnum:
		tableColumn.Type = TypeEnum
	case mysql.TypeSet:
		tableColumn.Type = TypeSet
	case mysql.TypeTimestamp:
		tableColumn.Type = TypeTimestamp
	case mysql.TypeDatetime:
		tableColumn.Type = TypeDatetime
	case mysql.TypeDuration:
		tableColumn.Type = TypeTime
	case mysql.TypeDouble, mysql.TypeFloat:
		tableColumn.Type = TypeFloat
	case mysql.TypeNewDecimal:
		tableColumn.Type = TypeDecimal
	case mysql.TypeBit:
		tableColumn.Type = TypeBit
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		if strings.HasPrefix(columnDef.Tp.String(), "binary") {
			tableColumn.Type = TypeBinary
		} else {
			tableColumn.Type = TypeString
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		tableColumn.Type = TypeNumber
	case mysql.TypeInt24:
		tableColumn.Type = TypeMediumInt
	case mysql.TypeDate:
		tableColumn.Type = TypeDate
	case mysql.TypeJSON:
		tableColumn.Type = TypeJson
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		if strings.HasSuffix(tableColumn.RawType, "text") {
			tableColumn.Type = TypeString
		} else {
			tableColumn.Type = TypeBinary
		}
	}
	for _, columnOption := range columnDef.Options {
		switch columnOption.Tp {
		case ast.ColumnOptionNoOption:
		case ast.ColumnOptionPrimaryKey:
			tableColumn.IsPrimaryKey = true
		case ast.ColumnOptionNotNull:
		case ast.ColumnOptionAutoIncrement:
		case ast.ColumnOptionDefaultValue:
		case ast.ColumnOptionUniqKey:
		case ast.ColumnOptionNull:
		case ast.ColumnOptionOnUpdate: // For Timestamp and Datetime only.
		case ast.ColumnOptionFulltext:
		case ast.ColumnOptionComment:
			switch exp := columnOption.Expr.(type) {
			case *test_driver.ValueExpr:
				tableColumn.Comment = exp.Datum.GetString()
			}
		case ast.ColumnOptionGenerated:
		case ast.ColumnOptionReference:
		case ast.ColumnOptionCollate:
		case ast.ColumnOptionCheck:
		case ast.ColumnOptionColumnFormat:
		case ast.ColumnOptionStorage:
		case ast.ColumnOptionAutoRandom:
		}
	}
	return tableColumn
}

func NewTable(createDdlSql string) (*Table, error) {
	tab := &Table{}
	err := TableDdlHandle(tab, createDdlSql)
	if err != nil {
		return nil, err
	}
	return tab, nil
}

func TableDdlHandle(tab *Table, sql string) error {
	astNode, err := parse(sql)
	if err != nil {
		return errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
	}
	switch t := (*astNode).(type) {
	case *ast.AlterTableStmt:
		if t.Table.Schema.String() != tab.Schema || t.Table.Name.String() != tab.Name {
			return errors.New(fmt.Sprintf("operation object do not match error: table: %s.%s and sql: %s", tab.Schema, tab.Name, sql))
		}
		for _, alterTableSpec := range t.Specs {
			switch alterTableSpec.Tp {
			case ast.AlterTableOption:
			case ast.AlterTableAddColumns:
				relativeColumn := ""
				isFirst := false
				switch alterTableSpec.Position.Tp {
				case ast.ColumnPositionNone:
				case ast.ColumnPositionFirst:
					isFirst = true
				case ast.ColumnPositionAfter:
					relativeColumn = alterTableSpec.Position.RelativeColumn.Name.String()
				}
				for _, column := range alterTableSpec.NewColumns {
					tableColumn := columnDefParse(column)
					if tableColumn.IsPrimaryKey {
						tab.PrimaryKeyColumns = append(tab.PrimaryKeyColumns, tableColumn)
					}
					if relativeColumn != "" {
						for i, column2 := range tab.Columns {
							// add new column to relative column after
							if column2.Name == relativeColumn {
								tab.Columns = append(tab.Columns[:i+1], append([]TableColumn{tableColumn}, tab.Columns[i+1:]...)...)
							}
						}
					} else if isFirst {
						// add new column to first
						tab.Columns = append([]TableColumn{tableColumn}, tab.Columns...)
					} else {
						tab.Columns = append(tab.Columns, tableColumn)
					}
				}
			case ast.AlterTableAddConstraint:
			case ast.AlterTableDropColumn:
				oldColumnName := alterTableSpec.OldColumnName.Name.String()
				for i, column := range tab.Columns {
					if column.Name == oldColumnName {
						tab.Columns = append(tab.Columns[:i], tab.Columns[i+1:]...)
					}
				}
			case ast.AlterTableDropPrimaryKey:
			case ast.AlterTableDropIndex:
			case ast.AlterTableDropForeignKey:
			case ast.AlterTableModifyColumn:
				relativeColumn := ""
				isFirst := false
				switch alterTableSpec.Position.Tp {
				case ast.ColumnPositionNone:
				case ast.ColumnPositionFirst:
					isFirst = true
				case ast.ColumnPositionAfter:
					relativeColumn = alterTableSpec.Position.RelativeColumn.Name.String()
				}
				for _, column := range alterTableSpec.NewColumns {
					tableColumn := columnDefParse(column)
					if tableColumn.IsPrimaryKey {
						tab.PrimaryKeyColumns = append(tab.PrimaryKeyColumns, tableColumn)
					}
					if relativeColumn != "" {
						for i, column2 := range tab.Columns {
							// delete old column
							if column2.Name == tableColumn.Name {
								tab.Columns = append(tab.Columns[:i], tab.Columns[i+1:]...)
							}
						}
						for i, column2 := range tab.Columns {
							// add new column to relative column after
							if column2.Name == relativeColumn {
								tab.Columns = append(tab.Columns[:i+1], append([]TableColumn{tableColumn}, tab.Columns[i+1:]...)...)
							}
						}
					} else if isFirst {
						for i, column2 := range tab.Columns {
							// delete old column
							if column2.Name == tableColumn.Name {
								tab.Columns = append(tab.Columns[:i], tab.Columns[i+1:]...)
							}
						}
						// add new column to first
						tab.Columns = append([]TableColumn{tableColumn}, tab.Columns...)
					} else {
						// overwrite column
						for i, column2 := range tab.Columns {
							if column2.Name == tableColumn.Name {
								tab.Columns[i] = tableColumn
								break
							}
						}
					}
				}
			case ast.AlterTableChangeColumn:
				oldColumnName := alterTableSpec.OldColumnName.Name.String()
				relativeColumn := ""
				isFirst := false
				switch alterTableSpec.Position.Tp {
				case ast.ColumnPositionNone:
				case ast.ColumnPositionFirst:
					isFirst = true
				case ast.ColumnPositionAfter:
					relativeColumn = alterTableSpec.Position.RelativeColumn.Name.String()
				}
				for _, column := range alterTableSpec.NewColumns {
					tableColumn := columnDefParse(column)
					if tableColumn.IsPrimaryKey {
						tab.PrimaryKeyColumns = append(tab.PrimaryKeyColumns, tableColumn)
					}
					if relativeColumn != "" {
						for i, column2 := range tab.Columns {
							// delete old column
							if column2.Name == oldColumnName {
								tab.Columns = append(tab.Columns[:i], tab.Columns[i+1:]...)
							}
						}
						for i, column2 := range tab.Columns {
							// add new column to relative column after
							if column2.Name == relativeColumn {
								tab.Columns = append(tab.Columns[:i+1], append([]TableColumn{tableColumn}, tab.Columns[i+1:]...)...)
							}
						}
					} else if isFirst {
						for i, column2 := range tab.Columns {
							// delete old column
							if column2.Name == oldColumnName {
								tab.Columns = append(tab.Columns[:i], tab.Columns[i+1:]...)
							}
						}
						// add new column to first
						tab.Columns = append([]TableColumn{tableColumn}, tab.Columns...)
					} else {
						// overwrite column
						for i, column2 := range tab.Columns {
							if column2.Name == oldColumnName {
								tab.Columns[i] = tableColumn
								break
							}
						}
					}
				}
			case ast.AlterTableRenameColumn:
				oldColumnName := alterTableSpec.OldColumnName.Name.String()
				newColumnName := alterTableSpec.NewColumnName.Name.String()
				for i, column := range tab.Columns {
					if column.Name == oldColumnName {
						tab.Columns[i].Name = newColumnName
						break
					}
				}
			case ast.AlterTableRenameTable:
				newTableName := alterTableSpec.NewTable.Name.String()
				newSchemaName := alterTableSpec.NewTable.Schema.String()
				tab.Name = newTableName
				if newSchemaName != "" {
					tab.Schema = newSchemaName
				}
			case ast.AlterTableAlterColumn:
			case ast.AlterTableLock:
			case ast.AlterTableWriteable:
			case ast.AlterTableAlgorithm:
			case ast.AlterTableRenameIndex:
			case ast.AlterTableForce:
			case ast.AlterTableAddPartitions:
			case ast.AlterTablePartitionAttributes:
			case ast.AlterTablePartitionOptions:
			case ast.AlterTableCoalescePartitions:
			case ast.AlterTableDropPartition:
			case ast.AlterTableTruncatePartition:
			case ast.AlterTablePartition:
			case ast.AlterTableEnableKeys:
			case ast.AlterTableDisableKeys:
			case ast.AlterTableRemovePartitioning:
			case ast.AlterTableWithValidation:
			case ast.AlterTableWithoutValidation:
			case ast.AlterTableSecondaryLoad:
			case ast.AlterTableSecondaryUnload:
			case ast.AlterTableRebuildPartition:
			case ast.AlterTableReorganizePartition:
			case ast.AlterTableCheckPartitions:
			case ast.AlterTableExchangePartition:
			case ast.AlterTableOptimizePartition:
			case ast.AlterTableRepairPartition:
			case ast.AlterTableImportPartitionTablespace:
			case ast.AlterTableDiscardPartitionTablespace:
			case ast.AlterTableAlterCheck:
			case ast.AlterTableDropCheck:
			case ast.AlterTableImportTablespace:
			case ast.AlterTableDiscardTablespace:
			case ast.AlterTableIndexInvisible:
			case ast.AlterTableOrderByColumns:
			case ast.AlterTableSetTiFlashReplica:
			case ast.AlterTableAddStatistics:
			case ast.AlterTableDropStatistics:
			case ast.AlterTableAttributes:
			case ast.AlterTableCache:
			case ast.AlterTableNoCache:
			case ast.AlterTableStatsOptions:
			case ast.AlterTableDropFirstPartition:
			case ast.AlterTableAddLastPartition:
			case ast.AlterTableReorganizeLastPartition:
			case ast.AlterTableReorganizeFirstPartition:
			case ast.AlterTableRemoveTTL:
			}
		}
	case *ast.CreateTableStmt:
		if tab.Schema != "" || tab.Name != "" {
			return errors.New("table object not empty error: create table sql table object must be empty")
		}
		tab.Schema = t.Table.Schema.String()
		tab.Name = t.Table.Name.String()
		for _, tableOption := range t.Options {
			switch tableOption.Tp {
			case ast.TableOptionComment:
				tab.Comment = tableOption.StrValue
			default:
				continue
			}
		}
		tab.Columns = []TableColumn{}
		for _, columnDef := range t.Cols {
			tableColumn := columnDefParse(columnDef)
			if tableColumn.IsPrimaryKey {
				tab.PrimaryKeyColumns = append(tab.PrimaryKeyColumns, tableColumn)
			}
			tab.Columns = append(tab.Columns, tableColumn)
		}
		for _, constraint := range t.Constraints {
			switch constraint.Tp {
			case ast.ConstraintPrimaryKey:
				for _, key := range constraint.Keys {
					keyName := key.Column.Name.String()
					for i, column := range tab.Columns {
						if keyName == column.Name {
							tab.Columns[i].IsPrimaryKey = true
							tab.PrimaryKeyColumns = append(tab.PrimaryKeyColumns, tab.Columns[i])
							break
						}
					}
				}
			case ast.ConstraintKey:
			case ast.ConstraintIndex:
			case ast.ConstraintUniq:
			case ast.ConstraintUniqKey:
			case ast.ConstraintUniqIndex:
			case ast.ConstraintForeignKey:
			case ast.ConstraintFulltext:
			case ast.ConstraintCheck:
			default:
			}
		}
	case *ast.DropTableStmt:
	case *ast.RenameTableStmt:
		for _, tableToTable := range t.TableToTables {
			if tableToTable.OldTable.Schema.String() != tab.Schema || tableToTable.OldTable.Name.String() != tab.Name {
				return errors.New(fmt.Sprintf("operation object do not match error: table: %s.%s and sql: %s", tab.Schema, tab.Name, sql))
			}
			newSchema := tableToTable.NewTable.Schema.String()
			newTable := tableToTable.NewTable.Name.String()
			tab.Schema = newSchema
			tab.Name = newTable
			break
		}
	case *ast.TruncateTableStmt:
	default:
		fmt.Printf("%v\n", t)
	}
	return nil
}

func DdlToDdlStatements(sql string, schema string) ([]*DdlStatement, error) {
	astNode, err := parse(sql)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
	}
	ddls := make([]*DdlStatement, 0)
	switch t := (*astNode).(type) {
	case *ast.AlterTableStmt:
		ddl := &DdlStatement{}
		if t.Table.Schema.String() != "" {
			ddl.Schema = t.Table.Schema.String()
		} else {
			ddl.Schema = schema
			t.Table.Schema.L = schema
			t.Table.Schema.O = schema
		}
		ddl.Name = t.Table.Name.String()
		buf := new(bytes.Buffer)
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err = t.Restore(restoreCtx)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
		}
		ddl.RawSql = buf.String()
		ddls = append(ddls, ddl)
	case *ast.CreateTableStmt:
		ddl := &DdlStatement{}
		if t.Table.Schema.String() != "" {
			ddl.Schema = t.Table.Schema.String()
		} else {
			ddl.Schema = schema
			t.Table.Schema.L = schema
			t.Table.Schema.O = schema
		}
		ddl.Name = t.Table.Name.String()

		// create table xxx.xxx like xxx.xxx
		if t.ReferTable != nil {
			if t.ReferTable.Schema.String() != "" {
				ddl.ReferTable.Schema = t.ReferTable.Schema.String()
			} else {
				ddl.ReferTable.Schema = schema
				t.ReferTable.Schema.L = schema
				t.ReferTable.Schema.O = schema
			}
			ddl.ReferTable.Name = t.ReferTable.Name.String()
			ddl.IsLikeCreateTable = true
		}

		// create table xxx.xxx select * from xxx.xxx
		if t.Select != nil {
			ddl.IsSelectCreateTable = true
			buf := new(bytes.Buffer)
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
			err = t.Select.Restore(restoreCtx)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
			}
			ddl.SelectRawSql = buf.String()
		}

		buf := new(bytes.Buffer)
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err = t.Restore(restoreCtx)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
		}
		ddl.RawSql = buf.String()
		ddl.IsCreateTable = true

		ddls = append(ddls, ddl)
	case *ast.DropTableStmt:
		for _, tableName := range t.Tables {
			ddl := &DdlStatement{}
			if tableName.Schema.String() != "" {
				ddl.Schema = tableName.Schema.String()
			} else {
				ddl.Schema = schema
				tableName.Schema.L = schema
				tableName.Schema.O = schema
			}
			ddl.Name = tableName.Name.String()
			buf := new(bytes.Buffer)
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
			restoreCtx.WriteKeyWord("DROP TABLE ")
			err = tableName.Restore(restoreCtx)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
			}
			ddl.RawSql = buf.String()
			ddl.IsDropTable = true
			ddls = append(ddls, ddl)
		}
	case *ast.RenameTableStmt:
		for _, tableToTable := range t.TableToTables {
			ddl := &DdlStatement{}
			oldSchema := ""
			if tableToTable.OldTable.Schema.String() != "" {
				oldSchema = tableToTable.OldTable.Schema.String()
			}
			oldTable := tableToTable.OldTable.Name.String()
			if oldSchema != "" {
				ddl.Schema = oldSchema
				ddl.Name = oldTable
			} else {
				ddl.Schema = schema
				ddl.Name = oldTable
				tableToTable.OldTable.Schema.L = schema
				tableToTable.OldTable.Schema.O = schema
			}

			if tableToTable.NewTable.Schema.String() == "" {
				tableToTable.NewTable.Schema.L = schema
				tableToTable.NewTable.Schema.O = schema
			}

			buf := new(bytes.Buffer)
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
			restoreCtx.WriteKeyWord("RENAME TABLE ")
			err = tableToTable.Restore(restoreCtx)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
			}
			ddl.RawSql = buf.String()
			ddl.IsRenameTable = true
			ddls = append(ddls, ddl)
		}
	case *ast.TruncateTableStmt:
		ddl := &DdlStatement{}
		if t.Table.Schema.String() != "" {
			ddl.Schema = t.Table.Schema.String()
		} else {
			ddl.Schema = schema
			t.Table.Schema.L = schema
			t.Table.Schema.O = schema
		}
		ddl.Name = t.Table.Name.String()
		buf := new(bytes.Buffer)
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err = t.Restore(restoreCtx)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("parse error: %v\n", err.Error()))
		}
		ddl.RawSql = buf.String()
		ddls = append(ddls, ddl)
	default:
		fmt.Printf("%v\n", t)
	}
	return ddls, nil
}
