package usage

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/valri11/distributedcounter/publisher"
	"github.com/valri11/distributedcounter/types"
)

type MessagePublisher interface {
	ReportUsage(ctx context.Context, usage []types.AccountUsage) error
}

type usageReporter struct {
	publisher        MessagePublisher
	publisherOptions map[string]string
	delayReportTime  time.Duration
	mx               *sync.RWMutex
	snapshot         map[string]int64
	snapshotRegion   string
}

func WithDelayReport(ts time.Duration) func(*usageReporter) {
	return func(s *usageReporter) {
		s.delayReportTime = ts
	}
}

func WithPublisherParams(params map[string]string) func(*usageReporter) {
	return func(s *usageReporter) {
		s.publisherOptions = params
	}
}

func NewUsageReporter(publisherType string,
	url string, options ...func(*usageReporter)) (*usageReporter, error) {

	ur := &usageReporter{
		mx:       &sync.RWMutex{},
		snapshot: make(map[string]int64),
	}

	for _, opt := range options {
		opt(ur)
	}

	var pub MessagePublisher
	var err error
	switch publisherType {
	case "http":
		pub, err = publisher.NewHttpPublisher(url)
		if err != nil {
			return nil, err
		}
	case "amqp":
		pub, err = publisher.NewAmqpPublisher(url, ur.publisherOptions)
		if err != nil {
			return nil, err
		}
	case "kafka":
		pub, err = publisher.NewKafkaPublisher(url, ur.publisherOptions)
		if err != nil {
			return nil, err
		}
	default:
		panic("unknown publisher type")
	}
	ur.publisher = pub

	if ur.delayReportTime != 0 {
		go ur.doDelayedReporting()
	}

	return ur, nil
}

func (ur *usageReporter) ReportUsage(ctx context.Context,
	region string, accountID string, resourceUsage int64) error {
	if ur.delayReportTime == 0 {
		resUsage := []types.AccountUsage{
			{
				Region:    region,
				AccountID: accountID,
				TS:        time.Now().UnixMilli(),
				Counter:   resourceUsage,
			},
		}

		err := ur.publisher.ReportUsage(ctx, resUsage)
		return err

	} else {
		ur.mx.Lock()
		defer ur.mx.Unlock()

		ur.snapshotRegion = region
		ur.snapshot[accountID] += resourceUsage
	}

	return nil
}

func (ur *usageReporter) doDelayedReporting() {
	ticker := time.NewTicker(ur.delayReportTime)

	ctx := context.Background()
	for range ticker.C {
		ts := time.Now().UnixMilli()
		var resUsage []types.AccountUsage

		ur.mx.Lock()
		for k, v := range ur.snapshot {
			resUsage = append(resUsage, types.AccountUsage{
				Region:    ur.snapshotRegion,
				AccountID: k,
				TS:        ts,
				Counter:   v,
			})
		}
		ur.snapshot = make(map[string]int64)
		ur.mx.Unlock()

		if len(resUsage) > 0 {
			err := ur.publisher.ReportUsage(ctx, resUsage)
			if err != nil {
				slog.Error("report usage", "error", err)
			}
		}
	}
}
