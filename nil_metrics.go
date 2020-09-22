package heartbeat

import "context"

type (
	NilMetrics struct {
	}
)

func NewNilMetrics() *NilMetrics {
	return &NilMetrics{}
}

func (m *NilMetrics) RecordConsumerRegistered(ctx context.Context, id string, current int) {
	return
}

func (m *NilMetrics) ResetConsumerRegistered(ctx context.Context) {
	return
}
