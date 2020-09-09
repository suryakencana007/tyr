/*  raw_test.go
*
* @Author:             Nanang Suryadi
* @Date:               November 24, 2019
* @Last Modified by:   @suryakencana007
* @Last Modified time: 24/11/19 22:09
 */

package tyr

import (
	"testing"
	"time"

	"github.com/suryakencana007/mimir"

	"github.com/stretchr/testify/assert"
)

func TestRawQuery_AND_OR(t *testing.T) {
	rawSelect := Build()
	query, args := rawSelect.From(Game{}, "g").And(&Game{Code: "code", Enabled: true}, "g").Or(&Game{ID: 1, Code: "code"}, "g").ToSQL()
	assert.Contains(t, query, "SELECT g.* FROM ref_game g WHERE g.enabled = $1 AND g.game_code = $2 OR g.game_code = $3 OR g.game_id = $4 LIMIT 100 OFFSET 0")
	assert.Equal(t, 4, len(args))
}

func TestRawQuery_AND(t *testing.T) {
	rawSelect := Build()
	query, args := rawSelect.From(Game{}, "g").And(&Game{Code: "code", Enabled: true}, "g").ToSQL()
	assert.Contains(t, query, "SELECT g.* FROM ref_game g WHERE g.enabled = $1 AND g.game_code = $2 LIMIT 100 OFFSET 0")
	assert.Equal(t, 2, len(args))
}

func TestRawQuery_OR(t *testing.T) {
	rawSelect := Build()
	query, args := rawSelect.From(Game{}, "g").Or(&Game{Code: "code", Enabled: true}, "g").ToSQL()
	assert.Contains(t, query, "SELECT g.* FROM ref_game g WHERE g.enabled = $1 OR g.game_code = $2 LIMIT 100 OFFSET 0")
	assert.Equal(t, 2, len(args))
}

func TestRawQuery_Join(t *testing.T) {
	rawSelect := Build()
	query, args := rawSelect.From(Game{
		ID:   1,
		Code: "code",
	}, "g").Join(User{}, "u", "u.id = g.user_id").Where("u.name like %| ? |%", "budi").ToSQL()
	mimir.Info(query)
	assert.Contains(t, query, "SELECT g.*, u.* FROM ref_game g JOIN ref_user u ON u.id = g.user_id WHERE g.game_code = $1 AND g.game_id = $2 AND u.name like %| $3 |% LIMIT 100 OFFSET 0")
	assert.Equal(t, len(args), 3)
}

func TestRawQuery_From(t *testing.T) {
	game := &Game{
		ID:          507,
		Description: String("Froze"),
	}
	rawSelect := Build()
	query, args := rawSelect.From(game, "g").ToSQL()
	mimir.Info(query)
	assert.Contains(t, query, "SELECT g.* FROM ref_game g WHERE g.game_description = $1 AND g.game_id = $2 LIMIT 100 OFFSET 0")
	assert.Equal(t, len(args), 2)
}

func TestRawQuery_UpdatesFewField(t *testing.T) {
	game := &Game{
		ID:          507,
		Code:        mimir.UUID(),
		Title:       "DOTA2",
		Description: String("Froze"),
	}
	game.Enabled = true
	rawUpdates := Build()
	query, args := rawUpdates.Updates(game).Where("game_code = ? AND game_description > ?", game.Code, 23).ToSQL()
	t.Log(query)
	assert.Contains(t, query, "UPDATE ref_game SET enabled = $2, game_code = $3, game_description = $4, game_title = $5, write_date = $1 WHERE game_code = $6 AND game_description > $7 RETURNING game_id")
	assert.Equal(t, len(args), 7)
}

func TestRawQuery_Updates(t *testing.T) {
	game := newGame()
	rawUpdates := Build()
	query, args := rawUpdates.Updates(game).Where("game_code = ? AND game_description > ?", game.Code, 23).ToSQL()
	assert.Contains(t, query, "UPDATE ref_game SET enabled = $2, game_code = $3, game_description = $4, game_title = $5, rate = $6, release = $7, write_date = $1")
	assert.Equal(t, len(args), 9)
}

func TestRawQuery_Insert(t *testing.T) {
	game := newGame()
	raw := Build().SetTag("sql")
	query, args := raw.Insert(game).ToSQL()
	t.Log(query)
	assert.Contains(t, query, "INSERT INTO ref_game (enabled, game_code, game_description, game_id, game_title, rate, release, create_date, write_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
	assert.Equal(t, len(args), 9)
}

func TestRawQuery_Inserts(t *testing.T) {
	games := make([]*Game, 0)
	game := newGame()
	games = append(games, game)
	games = append(games, game)
	games = append(games, game)
	raw := Build().SetTag("sql")
	query, args := raw.Inserts(games).ToSQL()
	t.Log(query)
	assert.Contains(t, query, "INSERT INTO ref_game (enabled, game_code, game_description, game_id, game_title, rate, release, create_date, write_date) VALUES")
	assert.Equal(t, len(args), 27)
}

func newGame() *Game {
	return &Game{
		ID:          507,
		Code:        mimir.UUID(),
		Title:       "DOTA2",
		Description: String("08675467484389"),
		Enabled:     true,
		Rate:        Int64(75),
		Release:     Time(time.Now().UTC()),
	}
}

type Game struct {
	CreatedAt   time.Time  `json:"create_date,omitempty"`
	CreatedBy   string     `json:"created_by,omitempty"`
	UpdatedAt   time.Time  `json:"write_date,omitempty"`
	UpdatedBy   string     `json:"updated_by,omitempty"`
	DeletedAt   time.Time  `json:"deleted_at,omitempty"`
	ID          int        `json:"game_id" sql:"game_id"`
	Code        string     `json:"game_code" sql:"game_code"`
	Title       string     `json:"game_title" sql:"game_title"`
	Description NullString `json:"game_description" sql:"game_description"`
	Enabled     bool       `json:"enabled" sql:"enabled"`
	Rate        NullInt64  `json:"rate" sql:"rate"`
	Release     NullTime   `json:"release" sql:"release"`
}

func (Game) TableName() string {
	return "ref_game"
}

type User struct {
	CreatedAt time.Time `json:"create_date,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
	UpdatedAt time.Time `json:"write_date,omitempty"`
	UpdatedBy string    `json:"updated_by,omitempty"`
	DeletedAt time.Time `json:"deleted_at,omitempty"`
	ID        int       `json:"user_id" sql:"id"`
	Name      string    `json:"user_name" sql:"name"`
}

func (User) TableName() string {
	return "ref_user"
}

type Member struct {
	CreatedAt time.Time `json:"create_date,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
	UpdatedAt time.Time `json:"write_date,omitempty"`
	UpdatedBy string    `json:"updated_by,omitempty"`
	DeletedAt time.Time `json:"deleted_at,omitempty"`
	ID        int       `json:"user_id" sql:"id"`
	Name      string    `json:"user_name" sql:"name"`
}

func (Member) TableName() string {
	return "ref_member"
}
