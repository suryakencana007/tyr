/*  query.go
*
* @Author:             Nanang Suryadi
* @Date:               November 24, 2019
* @Last Modified by:   @suryakencana007
* @Last Modified time: 24/11/19 22:00
 */

package tyr

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/suryakencana007/mimir"
)

type formatField func(key string, n int, opts tagOptions) string

type Model interface {
	TableName() string
}

type Query struct {
	raw    *SQL
	Query  string
	Args   []interface{}
	Rows   int
	Offset int
}

func Build() *Query {
	return &Query{}
}

func (s *Query) NewScope(model interface{}) *SQL {
	if s.raw == nil {
		s.raw = &SQL{query: s.clone(), Model: model, TagName: "sql"}
	} else {
		s.raw.Model = model
	}
	return s.raw
}

func (s *Query) SetTag(tag string) *Query {
	s.clone().raw.TagName = tag
	return s
}
func (s *Query) Limit(limit int) *Query {
	s.Rows = limit
	return s
}

func (s *Query) Page(page int) *Query {
	s.Offset = page
	return s
}

func (s *Query) Or(model interface{}, alias string) *Query {
	return s.clone().raw.operator(model, alias, "OR").query
}

func (s *Query) And(model interface{}, alias string) *Query {
	return s.clone().raw.operator(model, alias, "AND").query
}

// join ref_user u on u.id = g.user_id
func (s *Query) Join(model interface{}, alias string, on ...interface{}) *Query {
	return s.clone().raw.join(model, alias, on...).query
}

func (s *Query) From(model interface{}, alias string) *Query {
	return s.NewScope(model).fromQuery(alias).query
}

func (s *Query) Insert(model interface{}) *Query {
	return s.NewScope(model).insert().query
}

func (s *Query) Inserts(model interface{}) *Query {
	return s.NewScope(model).inserts().query
}

func (s *Query) Updates(model interface{}) *Query {
	return s.NewScope(model).updates().query
}

func (s *Query) Where(query interface{}, args ...interface{}) *Query {
	return s.clone().raw.Where(query, args...).query
}

func (s *Query) ToSQL() (string, []interface{}) {
	s.raw.Exec()
	if strings.Contains(strings.ToLower(s.Query), "update") || strings.Contains(strings.ToLower(s.Query), "insert") {
		var buff strings.Builder
		buff.Reset()
		buff.WriteString(s.Query)
		buff.WriteString(" RETURNING ")
		buff.WriteString(s.raw.ID)
		s.Query = buff.String()
		buff.Reset()
	}
	return s.Query, s.Args
}

func (s *Query) clone() *Query {
	if s.raw == nil {
		s.raw = &SQL{query: s, TagName: "sql"}
	}
	return s
}

type SQL struct {
	Model           interface{}
	ID              string
	TagName         string
	query           *Query
	whereConditions []map[string]interface{}
}

func (r *SQL) Where(query interface{}, values ...interface{}) *SQL {
	if len(r.whereConditions) > 0 {
		query = fmt.Sprintf(" AND %s", query)
	}
	r.whereConditions = append(r.whereConditions, map[string]interface{}{"query": query, "args": values})
	return r
}

func (r *SQL) Exec() {
	var buff strings.Builder
	buff.WriteString(r.query.Query)
	if len(r.whereConditions) > 0 {
		buff.WriteString(" WHERE ")
		for _, w := range r.whereConditions {
			lenArgs := len(r.query.Args)
			query := w["query"].(string)
			lenQuestion := strings.Count(query, "?")
			for i := 1; i <= lenQuestion; i++ {
				query = strings.Replace(query, "?", fmt.Sprintf("$%d", lenArgs+i), 1)
			}
			buff.WriteString(query)
			args := w["args"].([]interface{})
			r.query.Args = append(r.query.Args, args...)
		}
	}
	if strings.Contains(r.query.Query, "SELECT") {
		buff.WriteString(" LIMIT ")
		if r.query.Rows > 0 {
			buff.WriteString(strconv.Itoa(r.query.Rows))
		} else {
			buff.WriteString("100")
		}
		buff.WriteString(" OFFSET ")
		buff.WriteString(strconv.Itoa(r.query.Rows * (r.query.Offset - 1)))
	}

	r.query.Query = buff.String()
	buff.Reset()
}

func (r *SQL) join(model interface{}, alias string, on ...interface{}) *SQL {
	if len(on) < 1 && !strings.Contains(r.query.Query, "SELECT") {
		panic("select syntax or join field not found")
	}

	// find "from" word on query
	var idx int
	if idx = strings.Index(r.query.Query, "FROM"); idx < 1 {
		panic("FROM syntax not found")
	}

	var buff strings.Builder
	buff.Reset()
	buff.WriteString(strings.TrimRight(r.query.Query[:idx], " ")) // select syntax
	buff.WriteString(", ")
	buff.WriteString(fmt.Sprintf("%s.* ", alias))
	buff.WriteString(r.query.Query[idx:]) //from syntax
	buff.WriteString(" JOIN ")
	buff.WriteString(fmt.Sprintf("%s %s", model.(Model).TableName(), alias))
	buff.WriteString(" ON ")
	for _, arg := range on {
		buff.WriteString(arg.(string)) // don't forget to assign alias
	}
	r.query.Query = buff.String()
	buff.Reset()
	return r
}

func (r *SQL) modelAlias(tableName string) string {
	return strings.Replace(fmt.Sprintf("%s ?", tableName), "?", mimir.ToCamel(tableName), -1)
}

func (r *SQL) operator(model interface{}, alias, operator string) *SQL {
	if !strings.Contains(r.query.Query, "SELECT") {
		panic("select syntax or join field not found")
	}

	columns, err := r.fieldsToArgs(
		model,
		func(key string, n int, opts tagOptions) string {
			return fmt.Sprintf("%s.%s = ?", alias, key)
		},
	)
	if err != nil {
		panic(err.Error())
	}

	if len(columns) > 0 {
		var buff strings.Builder
		buff.Reset()
		args := r.query.Args
		if len(r.whereConditions) > 0 {
			buff.WriteString(fmt.Sprintf(" %s ", operator))
		}
		buff.WriteString(strings.Join(columns, fmt.Sprintf(" %s ", operator)))
		r.whereConditions = append(r.whereConditions, map[string]interface{}{"query": buff.String(), "args": args})
		buff.Reset()
		r.query.Args = nil
	}
	return r
}

func (r *SQL) fromQuery(alias string) *SQL {
	columns, err := r.fieldsToArgs(
		r.Model,
		func(key string, n int, opts tagOptions) string {
			return fmt.Sprintf("%s.%s = ?", alias, key)
		},
	)
	if err != nil {
		panic(err.Error())
	}
	var buff strings.Builder
	buff.Reset()
	buff.WriteString("SELECT ")
	buff.WriteString(fmt.Sprintf("%s.* ", alias))
	buff.WriteString("FROM ")
	buff.WriteString(fmt.Sprintf("%s %s", r.Model.(Model).TableName(), alias))
	r.query.Query = buff.String()
	buff.Reset()
	if len(columns) > 0 {
		args := r.query.Args
		r.whereConditions = append(r.whereConditions, map[string]interface{}{"query": strings.Join(columns, " AND "), "args": args})
		r.query.Args = nil
	}
	return r
}

func (r *SQL) insert() *SQL {
	columns, err := r.fieldsToArgs(
		r.Model,
		func(key string, n int, opts tagOptions) string {
			return key
		},
	)
	if err != nil {
		panic(err.Error())
	}
	columns = append(columns, "create_date", "write_date")
	r.query.Args = append(r.query.Args, time.Now().UTC(), time.Now().UTC())
	params := make([]string, 0)
	for i := 1; i <= len(r.query.Args); i++ {
		params = append(params, fmt.Sprintf(`$%d`, i))
	}
	var buff strings.Builder
	buff.Reset()
	buff.WriteString("INSERT INTO ")
	buff.WriteString(r.Model.(Model).TableName())
	buff.WriteString(" (")
	buff.WriteString(strings.Join(columns, ", "))
	buff.WriteString(") ")
	buff.WriteString("VALUES (")
	buff.WriteString(strings.Join(params, ", "))
	buff.WriteString(")")
	r.query.Query = buff.String()
	buff.Reset()
	return r
}

func (r *SQL) inserts() *SQL {
	var buff strings.Builder
	buff.Reset()
	t := reflect.TypeOf(r.Model)
	val := reflect.ValueOf(r.Model)
	rows := make([][]string, 0)
	if t.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			model := val.Index(i).Interface()
			f, err := r.fieldsToArgs(model, func(key string, n int, opts tagOptions) string {
				return fmt.Sprintf(`$%d`, n)
			})
			if err != nil {
				panic(err.Error())
			}
			f = append(f, fmt.Sprintf(`$%d`, len(r.query.Args)+1), fmt.Sprintf(`$%d`, len(r.query.Args)+2))
			r.query.Args = append(r.query.Args, time.Now().UTC(), time.Now().UTC())

			rows = append(rows, f)
		}
	}
	cols, err := r.TagsToField(r.TagName, val.Index(0).Interface())
	if err != nil {
		panic(err.Error())
	}
	keys := make([]string, 0)
	for k := range cols {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	keys = append(keys, "create_date", "write_date")

	buff.Reset()
	for n, columns := range rows {
		buff.WriteString("(")
		for m, column := range columns {
			buff.WriteString(column)
			if m < len(columns)-1 {
				buff.WriteString(", ")
			}
		}
		buff.WriteString(")")
		if n < len(rows)-1 {
			buff.WriteString(", ")
		}
	}
	value := buff.String()
	buff.Reset()
	buff.WriteString("INSERT INTO ")
	buff.WriteString(val.Index(0).Interface().(Model).TableName())
	buff.WriteString(" (")
	buff.WriteString(strings.Join(keys, ", "))
	buff.WriteString(") ")
	buff.WriteString("VALUES ")
	buff.WriteString(value)
	r.query.Query = buff.String()
	return r
}

func (r *SQL) updates() *SQL {
	var buff strings.Builder
	buff.Reset()

	r.query.Args = append(r.query.Args, time.Now().UTC())
	columns, err := r.fieldsToArgs(
		r.Model,
		func(key string, n int, opts tagOptions) string {
			if !strings.Contains(key, r.ID) {
				return fmt.Sprintf(`%s = $%d`, key, n)
			}
			return ""
		},
	)
	if err != nil {
		panic(err.Error())
	}
	columns = append(columns, "write_date = $1")
	buff.WriteString("UPDATE ")
	buff.WriteString(r.Model.(Model).TableName())
	buff.WriteString(" SET ")
	buff.WriteString(strings.Join(columns, ", "))
	r.query.Query = buff.String()
	buff.Reset()
	return r
}

func (r *SQL) fieldsToArgs(model interface{}, fn formatField) ([]string, error) {
	fields, err := r.TagsToField(r.TagName, model)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0)
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	f := make([]string, 0)
	for _, k := range keys {
		field := fn(k, len(r.query.Args)+1, fields[k][1].(tagOptions))
		if len(field) < 1 {
			continue
		}
		f = append(f, field)
		arg, ok := fields[k][0].(string)
		if !ok {
			if integer, err := strconv.Atoi(fields[k][0].(string)); err == nil {
				r.query.Args = append(r.query.Args, integer)
				continue
			}
			if b, err := strconv.ParseBool(fields[k][0].(string)); err == nil {
				r.query.Args = append(r.query.Args, b)
				continue
			}
		}
		r.query.Args = append(r.query.Args, arg)
	}
	return f, nil
}

func (r *SQL) TagsToField(tag string, value interface{}) (result map[string][]interface{}, err error) {
	fn := func() (err error) {
		defer func() {
			if e := recover(); e != nil {
				err = e.(error)
			}
		}()
		_ = reflect.ValueOf(value).Elem()
		return nil
	}
	if fn() != nil {
		obj := reflect.New(reflect.TypeOf(value))
		obj.Elem().Set(reflect.ValueOf(value))
		value = obj.Interface()
	}
	result = make(map[string][]interface{})
	t := reflect.ValueOf(value).Elem()
	for i := 0; i < t.NumField(); i++ {
		val := t.Field(i)
		field := t.Type().Field(i)
		tagVal := field.Tag.Get(tag)
		name, opts := parseTag(tagVal)
		if !isValidTag(name) {
			name = ""
		}
		if strings.EqualFold(field.Name, "ID") {
			r.ID = name
		}
		if isEmptyValue(val) || len(tagVal) < 1 {
			continue
		}
		switch val.Interface().(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			result[name] = append(result[name], fmt.Sprintf("%v", val.Interface()))
		case *bool:
			result[name] = append(result[name], fmt.Sprintf("%v", val.Elem().Interface()))
		case NullString:
			if !val.Interface().(NullString).Valid {
				continue
			}
			result[name] = append(result[name], val.Interface().(NullString).String)
		case NullInt64:
			if !val.Interface().(NullInt64).Valid {
				continue
			}
			result[name] = append(result[name], fmt.Sprintf("%v", val.Interface().(NullInt64).Int64))
		case NullTime:
			if !val.Interface().(NullTime).Valid {
				continue
			}
			result[name] = append(result[name], fmt.Sprintf("%v", val.Interface().(NullTime).Time.UTC().Format(time.RFC3339)))
		case NullFloat64:
			if !val.Interface().(NullFloat64).Valid {
				continue
			}
			result[name] = append(result[name], fmt.Sprintf("%v", val.Interface().(NullFloat64).Float64))
		case NullBool:
			if !val.Interface().(NullBool).Valid {
				continue
			}
			result[name] = append(result[name], fmt.Sprintf("%v", val.Interface().(NullBool).Bool))
		case time.Time:
			result[name] = append(result[name], fmt.Sprintf("%v", val.Interface().(time.Time).UTC().Format(time.RFC3339)))
		default:
			result[name] = append(result[name], fmt.Sprintf("%v", val.Interface()))
		}
		result[name] = append(result[name], opts)
	}
	return result, nil
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:<=>?@[]^_{|}~ ", c):
			// Backslash and quote chars are reserved, but
			// otherwise any punctuation chars are allowed
			// in a tag name.
		case !unicode.IsLetter(c) && !unicode.IsDigit(c):
			return false
		}
	}
	return true
}
