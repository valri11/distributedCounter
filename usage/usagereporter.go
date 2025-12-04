package usage

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type usageReporter struct {
	client          *http.Client
	url             string
	delayReportTime time.Duration
	mx              *sync.RWMutex
	snapshot        map[string]int64
}

func WithDelayReport(ts time.Duration) func(*usageReporter) {
	return func(s *usageReporter) {
		s.delayReportTime = ts
	}
}

func NewUsageReporter(url string, options ...func(*usageReporter)) (*usageReporter, error) {
	client := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	ur := &usageReporter{
		client:   &client,
		url:      url,
		mx:       &sync.RWMutex{},
		snapshot: make(map[string]int64),
	}

	for _, opt := range options {
		opt(ur)
	}

	if ur.delayReportTime != 0 {
		go ur.doDelayedReporting()
	}

	return ur, nil
}

func (ur *usageReporter) ReportUsage(ctx context.Context,
	accountID string, resourceUsage int64) error {
	if ur.delayReportTime == 0 {
		resUsage := AccountUsage{
			AccountID: accountID,
			Counter:   resourceUsage,
		}

		// Marshal the user data into JSON
		jsonData, err := json.Marshal(resUsage)
		if err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx,
			http.MethodPost,
			ur.url,
			bytes.NewBuffer(jsonData))
		if err != nil {
			return err
		}

		// Set the Content-Type header to application/json
		req.Header.Set("Content-Type", "application/json")

		// Send the request
		resp, err := ur.client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
	} else {
		ur.mx.Lock()
		defer ur.mx.Unlock()

		ur.snapshot[accountID] += resourceUsage
	}

	return nil
}

func (ur *usageReporter) doDelayedReporting() {
	ticker := time.NewTicker(ur.delayReportTime)

	for range ticker.C {
		snapshot := make(map[string]int64)

		ur.mx.Lock()
		for k, v := range ur.snapshot {
			snapshot[k] = v
		}
		ur.snapshot = make(map[string]int64)
		ur.mx.Unlock()

		for k, v := range snapshot {
			resUsage := AccountUsage{
				AccountID: k,
				Counter:   v,
			}

			// Marshal the user data into JSON
			jsonData, err := json.Marshal(resUsage)
			if err != nil {
				//return err
			}

			req, err := http.NewRequest(
				http.MethodPost,
				ur.url,
				bytes.NewBuffer(jsonData))
			if err != nil {
				//return err
			}

			// Set the Content-Type header to application/json
			req.Header.Set("Content-Type", "application/json")

			// Send the request
			resp, err := ur.client.Do(req)
			if err != nil {
				//return err
			}
			resp.Body.Close()
		}
	}
}
