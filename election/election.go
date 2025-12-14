package election

import (
	"context"
	"log/slog"
	"time"

	leaderelection "github.com/drio-ai/leaderelection"
	leaderElectionWithRedis "github.com/drio-ai/leaderelection/redis"
	"github.com/redis/go-redis/v9"

	"github.com/valri11/distributedcounter/config"
)

type ElectionManager struct {
	leaderElection leaderelection.LeaderElector
}

func New(ctx context.Context,
	cfg config.LeaderElectionConfig,
	leaderCallback func(ctx context.Context) error,
	followerCallback func(ctx context.Context) error,
) (*ElectionManager, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.URL,
		Password: "",
		DB:       0,
	})

	// Ping the Redis server to check the connection
	pong, err := redisClient.Ping(ctx).Result()
	if err != nil {
		slog.Error("error connecting to redis:", "error", err)
	} else {
		slog.Info("redis connection successful:", "ping", pong)
	}

	leaderElectionCfg := leaderElectionWithRedis.RedisLeaderElectionConfig{
		LeaderElectionConfig: leaderelection.LeaderElectionConfig{
			RelinquishInterval:    time.Duration(cfg.RelinquishInterval) * time.Second,
			LeaderCheckInterval:   time.Duration(cfg.LeaderCheckInterval) * time.Second,
			FollowerCheckInterval: time.Duration(cfg.FollowerCheckInterval) * time.Second,
			LeaderCallback:        leaderCallback,
			FollowerCallback:      followerCallback,
		},
	}

	leaderElection, err := leaderElectionWithRedis.NewWithConn(ctx, redisClient, leaderElectionCfg)
	if err != nil {
		return nil, err
	}

	em := ElectionManager{
		leaderElection: leaderElection,
	}
	return &em, nil
}

func (em *ElectionManager) Run(ctx context.Context) {
	go func(ctx context.Context) {
		em.leaderElection.Run(ctx)
	}(ctx)
}

func (em *ElectionManager) Close() {
	em.leaderElection.Close()
}
