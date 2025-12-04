package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/valri11/distributedcounter/types"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type httpPublisher struct {
	client *http.Client
	url    string
}

func NewHttpPublisher(url string) (*httpPublisher, error) {
	client := http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	p := httpPublisher{
		client: &client,
		url:    url,
	}
	return &p, nil
}

func (p *httpPublisher) ReportUsage(ctx context.Context, resUsage []types.AccountUsage) error {
	// Marshal the user data into JSON
	jsonData, err := json.Marshal(resUsage)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx,
		http.MethodPost,
		p.url,
		bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	// Set the Content-Type header to application/json
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
