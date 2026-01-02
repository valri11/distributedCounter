package types

type AccountUsage struct {
	Region    string `json:"region,omitempty" db:"region"`
	AccountID string `json:"account_id" db:"account_id"`
	TS        int64  `json:"ts,omitempty" db:"ts"`
	Counter   int64  `json:"counter" db:"counter"`
}
