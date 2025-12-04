package types

type AccountUsage struct {
	AccountID string `json:"account_id" db:"account_id"`
	Counter   int64  `json:"counter" db:"counter"`
}
