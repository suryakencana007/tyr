package tyr

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type nullStringRow struct {
	nullParam    NullString
	notNullParam string
	scanNullVal  interface{}
}

func TestNullStringParam(t *testing.T) {
	spec := []nullStringRow{
		{String("aqua"), "", String("aqua")},
		{String("brown"), "", NullString{sql.NullString{String: "brown", Valid: false}}},
		{String("chartreuse"), "", NullString{sql.NullString{String: "chartreuse", Valid: true}}},
		{String("darkred"), "", String("darkred")},
		{NullString{sql.NullString{String: "eel", Valid: false}}, "", NullString{sql.NullString{String: "", Valid: false}}},
		{String("foo"), "foo", nil},
	}
	for _, row := range spec {
		if row.nullParam.Valid {
			if row.scanNullVal != nil {
				assert.NotEqual(t, row.nullParam.String, row.notNullParam)
				b, e := json.Marshal(row.nullParam.String)
				assert.NoError(t, e)
				e = row.nullParam.UnmarshalJSON(b)
				assert.NoError(t, e)
				assert.Equal(t, row.nullParam.String, row.scanNullVal.(NullString).String)
			}
			b, er := row.nullParam.MarshalJSON()
			assert.NoError(t, er)
			var d string
			er = json.Unmarshal(b, &d)
			assert.NoError(t, er)
			assert.Equal(t, d, row.nullParam.String)
		} else {
			b, er := row.nullParam.MarshalJSON()
			assert.NoError(t, er)
			var d string
			er = json.Unmarshal(b, &d)
			assert.NoError(t, er)
			assert.Equal(t, d, row.notNullParam)
		}
	}
}

type nullInt64Row struct {
	nullParam    NullInt64
	notNullParam int64
	scanNullVal  interface{}
}

func TestNullInt64Param(t *testing.T) {
	spec := []nullInt64Row{
		{Int64(33), 0, Int64(33)},
		{Int64(77), 0, NullInt64{sql.NullInt64{Int64: 77, Valid: false}}},
		{Int64(87), 0, Int64(87)},
		{Int64(55), 0, Int64(55)},
		{NullInt64{sql.NullInt64{Int64: 0, Valid: false}}, 0, NullInt64{sql.NullInt64{Int64: 0, Valid: false}}},
		{Int64(64), 64, nil},
	}
	for _, row := range spec {
		if row.nullParam.Valid {
			if row.scanNullVal != nil {
				assert.NotEqual(t, row.nullParam.Int64, row.notNullParam)
				b, e := json.Marshal(row.nullParam.Int64)
				assert.NoError(t, e)
				e = row.nullParam.UnmarshalJSON(b)
				assert.NoError(t, e)
				assert.Equal(t, row.nullParam.Int64, row.scanNullVal.(NullInt64).Int64)
			}
			b, er := row.nullParam.MarshalJSON()
			assert.NoError(t, er)
			var d int64
			er = json.Unmarshal(b, &d)
			assert.NoError(t, er)
			assert.Equal(t, d, row.nullParam.Int64)
			assert.IsType(t, row.nullParam.Int(), 0)
		} else {
			b, er := row.nullParam.MarshalJSON()
			assert.NoError(t, er)
			var d int64
			er = json.Unmarshal(b, &d)
			assert.NoError(t, er)
			assert.Equal(t, d, row.notNullParam)
			assert.Equal(t, row.nullParam.Int(), 0)
		}
	}
}

type nullFloat64Row struct {
	nullParam    NullFloat64
	notNullParam float64
	scanNullVal  interface{}
}

func TestNullFloat64Param(t *testing.T) {
	spec := []nullFloat64Row{
		{Float64(33), 0, Float64(33)},
		{Float64(77), 0, NullFloat64{sql.NullFloat64{Float64: 77, Valid: false}}},
		{Float64(87), 0, Float64(87)},
		{Float64(55), 0, Float64(55)},
		{NullFloat64{sql.NullFloat64{Float64: 0, Valid: false}}, 0, NullFloat64{sql.NullFloat64{Float64: 0, Valid: false}}},
		{Float64(64), 64, nil},
	}
	for _, row := range spec {
		if row.nullParam.Valid {
			if row.scanNullVal != nil {
				assert.NotEqual(t, row.nullParam.Float64, row.notNullParam)
				b, e := json.Marshal(row.nullParam.Float64)
				assert.NoError(t, e)
				e = row.nullParam.UnmarshalJSON(b)
				assert.NoError(t, e)
				assert.Equal(t, row.nullParam.Float64, row.scanNullVal.(NullFloat64).Float64)
			}
			b, er := row.nullParam.MarshalJSON()
			assert.NoError(t, er)
			var d float64
			er = json.Unmarshal(b, &d)
			assert.NoError(t, er)
			assert.Equal(t, d, row.nullParam.Float64)
		} else {
			b, er := row.nullParam.MarshalJSON()
			assert.NoError(t, er)
			var d float64
			er = json.Unmarshal(b, &d)
			assert.NoError(t, er)
			assert.Equal(t, d, row.notNullParam)
		}
	}
}
