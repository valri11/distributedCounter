package usage

import (
	"context"
	"log/slog"
)

type UsageStore interface {
	RecordUsage(ctx context.Context, accountID string, counter int64) error
	UsageInfo(ctx context.Context) ([]AccountUsage, error)
}

type UsageManager struct {
	store UsageStore
}

func NewUsageManager(store UsageStore) (*UsageManager, error) {
	m := UsageManager{
		store: store,
	}
	return &m, nil
}

func (um *UsageManager) RecordUsage(ctx context.Context, accountID string, counter int64) error {
	err := um.store.RecordUsage(ctx, accountID, counter)
	if err != nil {
		slog.Error("record usage", "error", err)
	}
	return err
}

func (um *UsageManager) UsageInfo(ctx context.Context) ([]AccountUsage, error) {
	au, err := um.store.UsageInfo(ctx)
	if err != nil {
		slog.Error("account usage", "error", err)
	}
	return au, err
}
