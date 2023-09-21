package filter

import (
	"fmt"
	"github.com/dop251/goja"
	"github.com/liuxinwang/go-mysql-starrocks/pkg/msg"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-log/log"
	"os"
)

const JsDmlColumnFilterName = "js-dml-column"

type JsDmlColumnFilter struct {
	name        string
	matchSchema string
	matchTable  string
	JsFile      string
	JsVm        *goja.Runtime
	processRow  func(map[string]interface{}) *goja.Object
}

func (jdcf *JsDmlColumnFilter) NewFilter(config map[string]interface{}) error {
	jsFile, ok := config["js-file"]
	if !ok {
		return errors.Trace(errors.New("'js-file' is not configured"))
	}
	jdcf.name = JsDmlColumnFilterName
	jdcf.matchSchema = fmt.Sprintf("%v", config["match-schema"])
	jdcf.matchTable = fmt.Sprintf("%v", config["match-table"])
	jdcf.JsFile = fmt.Sprintf("%v", jsFile)
	err := jdcf.loadJs()
	if err != nil {
		return err
	}
	return nil
}

func (jdcf *JsDmlColumnFilter) Filter(msg *msg.Msg) bool {
	if jdcf.matchSchema == msg.Database && jdcf.matchTable == msg.Table {
		rs := jdcf.processRow(msg.DmlMsg.Data)
		if rs != nil {
			return true
		}
		var res map[string]interface{}
		err := jdcf.JsVm.ExportTo(rs, &res)
		if err != nil {
			log.Fatal(err)
		}
		msg.DmlMsg.Data = res
	}
	return false
}

func (jdcf *JsDmlColumnFilter) loadJs() error {
	file, err := os.ReadFile(jdcf.JsFile)
	if err != nil {
		return err
	}
	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	_, err = vm.RunString(string(file))
	if err != nil {
		return err
	}
	err = vm.ExportTo(vm.Get("process_row"), &jdcf.processRow)
	if err != nil {
		return err
	}
	jdcf.JsVm = vm
	return nil
}
