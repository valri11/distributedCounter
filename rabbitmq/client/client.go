package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type AmqpClient struct {
	client *http.Client

	baseURL  string
	username string
	password string
}

func NewClient(baseURL string, username string, password string) (*AmqpClient, error) {

	httpClient := http.Client{Timeout: time.Duration(1) * time.Second}

	c := AmqpClient{
		client: &httpClient,

		baseURL:  baseURL,
		username: username,
		password: password,
	}

	return &c, nil
}

/*
curl -sL -u ratelimiter:pass -H "Accept: application/json" http://127.0.0.1:15672/api/exchanges/ratelimiter/resourcecount/bindings/source | jq
[
  {
    "source": "resourcecount",
    "vhost": "ratelimiter",
    "destination": "usagedata01",
    "destination_type": "queue",
    "routing_key": "1",
    "arguments": {},
    "properties_key": "1"
  }
]
*/

type ExchangeSourceQueue struct {
	Source          string `json:"source"`
	VHost           string `json:"vhost"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
	Arguments       any    `json:"arguments"`
	PropertiesKey   string `json:"properties_key"`
}

type ExchangeSourceQueuesResponse []ExchangeSourceQueue

func (c *AmqpClient) GetExchangeSourceQueues(ctx context.Context, vhost string, exchangeName string) ([]string, error) {
	endpointURL := fmt.Sprintf("%s/api/exchanges/%s/%s/bindings/source",
		c.baseURL,
		vhost,
		exchangeName,
	)

	req, err := http.NewRequest("GET", endpointURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/json")
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var res ExchangeSourceQueuesResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	var queues []string
	for _, q := range res {
		queues = append(queues, q.Destination)
	}

	return queues, nil
}
