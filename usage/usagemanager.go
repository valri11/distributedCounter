package usage

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/valri11/distributedcounter/subscriber"
	"github.com/valri11/distributedcounter/types"
)

type UsageStore interface {
	RecordUsage(ctx context.Context, region string, accountID string, ts int64, counter int64) error
	UsageInfo(ctx context.Context) ([]types.AccountUsage, error)
}

type UsageManager struct {
	store    UsageStore
	cmsStore UsageStore
}

func NewUsageManager(store UsageStore, cmsStore UsageStore) (*UsageManager, error) {
	m := UsageManager{
		store:    store,
		cmsStore: cmsStore,
	}
	return &m, nil
}

func (um *UsageManager) RecordUsage(ctx context.Context,
	region string, accountID string, ts int64, counter int64) error {

	err := um.store.RecordUsage(ctx, region, accountID, ts, counter)
	if err != nil {
		slog.Error("record usage", "error", err)
		return err
	}

	err = um.cmsStore.RecordUsage(ctx, region, accountID, ts, counter)
	if err != nil {
		slog.Error("record usage CMS store", "error", err)
		return err
	}

	return nil
}

func (um *UsageManager) UsageInfo(ctx context.Context) ([]types.AccountUsage, error) {
	au, err := um.store.UsageInfo(ctx)
	if err != nil {
		slog.Error("account usage", "error", err)
	}
	return au, err
}

func (um *UsageManager) ApproxUsageInfo(ctx context.Context) ([]types.AccountUsage, error) {
	au, err := um.cmsStore.UsageInfo(ctx)
	if err != nil {
		slog.Error("approx account usage", "error", err)
	}
	return au, err
}

func (um *UsageManager) ProcessMessage(ctx context.Context, msg subscriber.Message) subscriber.MessageAction {
	var resourceUsage types.AccountUsage
	err := json.Unmarshal(msg.Body, &resourceUsage)
	if err != nil {
		return subscriber.NAckReject
	}

	slog.DebugContext(ctx, "set usage", "resourceUsage", resourceUsage)

	err = um.RecordUsage(ctx,
		resourceUsage.Region,
		resourceUsage.AccountID,
		resourceUsage.TS, resourceUsage.Counter)
	if err != nil {
		return subscriber.NAckReject
	}

	return subscriber.Ack
}
