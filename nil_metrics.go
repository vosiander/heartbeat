package heartbeat

type (
	NilMetrics struct {
	}
)

func NewNilMetrics() *NilMetrics {
	return &NilMetrics{}
}

func (m *NilMetrics) RecordConsumerRegistered(id string, current int) {
	return
}

func (m *NilMetrics) ResetConsumerRegistered() {
	return
}
