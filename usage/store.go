package usage

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/valri11/distributedcounter/types"
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

func (s *dbStore) RecordUsage(ctx context.Context,
	accountID string, ts int64, counter int64) error {
	cmd := `INSERT INTO usage (account_id, ts, counter) 
	VALUES (:account, :ts, :cnt)
	ON CONFLICT (account_id)
	DO UPDATE SET counter = usage.counter + excluded.counter,
	ts = excluded.ts
	WHERE usage.ts < excluded.ts`

	records := map[string]any{
		"account": accountID,
		"ts":      ts,
		"cnt":     counter,
	}

	_, err := s.db.NamedExecContext(ctx, cmd, records)

	return err
}

func (s *dbStore) UsageInfo(ctx context.Context) ([]types.AccountUsage, error) {
	cmd := `SELECT account_id, ts, counter FROM usage`

	var au []types.AccountUsage
	err := s.db.SelectContext(ctx, &au, cmd)
	return au, err
}
