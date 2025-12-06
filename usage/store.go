package usage

import (
	"context"
	"log/slog"

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
	region string, accountID string, ts int64, counter int64) error {
	cmd := `INSERT INTO usage (region, account_id, ts, counter, ts_history) 
	VALUES (:region, :account, :ts, :cnt, CAST(ARRAY[:ts] AS BIGINT[]))
	ON CONFLICT (region, account_id)
	DO UPDATE SET counter = usage.counter + excluded.counter,
	ts = GREATEST(usage.ts, excluded.ts),
	ts_history = array_append(usage.ts_history, excluded.ts)
	WHERE excluded.ts != ALL(usage.ts_history);`

	records := map[string]any{
		"region":  region,
		"account": accountID,
		"ts":      ts,
		"cnt":     counter,
	}

	res, err := s.db.NamedExecContext(ctx, cmd, records)
	if err != nil {
		slog.ErrorContext(ctx, "upsert counter", "error", err)
	}
	ra, err := res.RowsAffected()
	if err != nil {
		slog.ErrorContext(ctx, "upsert rows affected", "error", err)
	}
	if ra != 1 {
		slog.ErrorContext(ctx, "unexpected rows update", "rows", ra)
	}

	return err
}

func (s *dbStore) UsageInfo(ctx context.Context) ([]types.AccountUsage, error) {
	cmd := `SELECT region, account_id, ts, counter FROM usage`

	var au []types.AccountUsage
	err := s.db.SelectContext(ctx, &au, cmd)
	return au, err
}
