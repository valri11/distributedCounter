package usage

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type dbStore struct {
	db *sqlx.DB
}

func NewStore(db *sqlx.DB) (*dbStore, error) {
	s := dbStore{
		db: db,
	}
	return &s, nil
}

func (s *dbStore) RecordUsage(ctx context.Context, accountID string, counter int64) error {
	cmd := `INSERT INTO usage (account_id, counter) VALUES (:account, :cnt)
	ON CONFLICT (account_id)
	DO UPDATE SET counter = usage.counter + excluded.counter`

	records := map[string]any{
		"account": accountID,
		"cnt":     counter,
	}

	_, err := s.db.NamedExecContext(ctx, cmd, records)

	return err
}

func (s *dbStore) UsageInfo(ctx context.Context) ([]AccountUsage, error) {
	cmd := `SELECT account_id, counter FROM usage`

	var au []AccountUsage
	err := s.db.SelectContext(ctx, &au, cmd)
	return au, err
}
