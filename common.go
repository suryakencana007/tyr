package tyr

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
)

var (
	NotPointer = errors.New("must pass a pointer, not a value")
	NilPointer = errors.New("nil pointer passed")
)

func CloneStruct(src, dest interface{}) error {
	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr {
		return NotPointer
	}
	if d.IsNil() {
		return NilPointer
	}
	s := reflect.ValueOf(src)
	if s.Kind() != reflect.Ptr {
		return NotPointer
	}
	if s.IsNil() {
		return NilPointer
	}

	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dest)
}

func elemTypePtr(i interface{}) reflect.Type {
	t := reflect.TypeOf(i)
	switch {
	case t.Kind() == reflect.Ptr:
		t = t.Elem()
	case t.Kind() != reflect.Struct:
		panic(NotPointer)
	}
	return t
}

func colToFieldIndex(t reflect.Type, columns []string) ([][]int, error) {
	colToFieldIndex := make([][]int, len(columns))
	for x := range columns {
		colName := strings.ToLower(columns[x])
		field, found := t.FieldByNameFunc(func(fieldName string) bool {
			field, _ := t.FieldByName(fieldName)
			cArguments := strings.Split(field.Tag.Get("sql"), ",")
			fieldName = cArguments[0]

			if fieldName == "-" {
				return false
			} else if fieldName == "" {
				fieldName = field.Name
			}

			return colName == strings.ToLower(fieldName)
		})
		if found {
			colToFieldIndex[x] = field.Index
		}
	}
	return colToFieldIndex, nil
}

func ScanRow(rs *sql.Rows, dest interface{}) error {
	dpv := elemTypePtr(dest)

	columns, err := rs.Columns()
	if err != nil {
		return err
	}

	colToFieldIdx, erCol := colToFieldIndex(dpv, columns)
	if erCol != nil {
		return erCol
	}

	v := reflect.New(dpv)
	pointers := make([]interface{}, len(columns))
	for x := range columns {
		f := v.Elem()
		f = f.FieldByIndex(colToFieldIdx[x])
		target := f.Addr().Interface()
		switch f.Kind() {
		case reflect.String,
			reflect.Bool,
			reflect.Float64,
			reflect.Float32,
			reflect.Int,
			reflect.Int32,
			reflect.Int64:
			pointers[x] = target
			continue
		}
		switch f.Type() {
		case reflect.TypeOf(NullString{}),
			reflect.TypeOf(NullInt64{}),
			reflect.TypeOf(NullTime{}),
			reflect.TypeOf(NullFloat64{}),
			reflect.TypeOf(NullBool{}):
		default:
			pointers[x] = new(interface{})
			continue
		}
		pointers[x] = target
	}

	if er := rs.Scan(pointers...); er != nil {
		return er
	}

	dataMap := make(map[string]interface{}, len(columns))
	for idx, col := range columns {
		dataMap[col] = pointers[idx]
	}
	// struct scan
	if er := CloneStruct(&dataMap, &dest); er != nil {
		return er
	}
	return nil
}
