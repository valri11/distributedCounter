package usage

import (
	"context"
	"slices"

	"github.com/redis/go-redis/v9"
	"github.com/valri11/distributedcounter/types"
)

type CMSStore struct {
	url string

	client *redis.Client
}

func NewCMSStore(url string) (*CMSStore, error) {
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     url,
		Password: "",
		DB:       0,
	})

	// Ping the Redis server to check the connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	cmsExists := redisClient.Exists(ctx, "account-usage")
	if cmsExists.Err() != nil {
		return nil, cmsExists.Err()
	}

	keyExists, _ := cmsExists.Result()
	if keyExists == 0 {
		res := redisClient.CMSInitByProb(ctx, "account-usage", 0.001, 0.01)
		if res.Err() != nil {
			return nil, res.Err()
		}
	}

	s := CMSStore{
		url:    url,
		client: redisClient,
	}

	return &s, nil
}

func (s *CMSStore) RecordUsage(ctx context.Context, region string, accountID string, ts int64, counter int64) error {
	res := s.client.SAdd(ctx, "accounts", accountID)
	if res.Err() != nil {
		return res.Err()
	}

	resIncr := s.client.CMSIncrBy(ctx, "account-usage", accountID, counter)
	if resIncr.Err() != nil {
		return resIncr.Err()
	}

	return nil
}

func (s *CMSStore) UsageInfo(ctx context.Context) ([]types.AccountUsage, error) {
	res := s.client.SMembers(ctx, "accounts")
	if res.Err() != nil {
		return nil, res.Err()
	}

	accounts, err := res.Result()
	if err != nil {
		return nil, err
	}

	slices.Sort(accounts)

	var accountUsage []types.AccountUsage
	for _, acc := range accounts {
		res := s.client.CMSQuery(ctx, "account-usage", acc)
		if res.Err() != nil {
			return nil, err
		}

		accountUsage = append(accountUsage, types.AccountUsage{
			AccountID: acc,
			Counter:   res.Val()[0],
		})
	}

	return accountUsage, nil
}
