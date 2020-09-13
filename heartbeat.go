package heartbeat

import (
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const (
	PublisherTopic        = "peer-heartbeat"
	ConsumerRegisterTopic = "peer-consumer-register"
	TickerTime            = 30 * time.Second
)

type (
	Handler struct {
		id                      string
		nc                      *nats.Conn
		natsConsumer            []string
		rwlock                  *sync.RWMutex
		publisherHeartbeatTopic string
		consumerRegisterTopic   string
		heartbeatTicker         *time.Ticker
		heartbeatTickerTime     time.Duration
		logger                  logrus.FieldLogger
		metrics                 MetricsHandler
	}

	MetricsHandler interface {
		RecordConsumerRegistered(id string, current int)
		ResetConsumerRegistered()
	}

	Option func(h *Handler)
)

func NewHandler(id string, nc *nats.Conn, hos ...Option) *Handler {
	hh := &Handler{
		id:                      id,
		nc:                      nc,
		natsConsumer:            []string{},
		rwlock:                  &sync.RWMutex{},
		publisherHeartbeatTopic: PublisherTopic,
		consumerRegisterTopic:   ConsumerRegisterTopic,
		heartbeatTickerTime:     TickerTime,
		metrics:                 NewNilMetrics(),
		logger:                  logrus.WithField("component", "heartbeat"),
	}

	for _, o := range hos {
		o(hh)
	}

	return hh
}

func SetHeartbeatTopic(topic string) Option {
	return func(h *Handler) {
		h.publisherHeartbeatTopic = topic
	}
}

func SetConsumerRegisterTopic(topic string) Option {
	return func(h *Handler) {
		h.consumerRegisterTopic = topic
	}
}

func SetHeartbeatTickerTime(ti time.Duration) Option {
	return func(h *Handler) {
		h.heartbeatTickerTime = ti
	}
}

func SetLogger(l logrus.FieldLogger) Option {
	return func(h *Handler) {
		h.logger = l
	}
}

func SetMetrics(m MetricsHandler) Option {
	return func(h *Handler) {
		h.metrics = m
	}
}

func (h *Handler) ListenForConsumers() {
	_, err := h.nc.Subscribe(h.consumerRegisterTopic, func(m *nats.Msg) {
		h.addConsumer(string(m.Data))
		h.logger.WithField("msg", string(m.Data)).Trace("consumer registered")
	})
	if err != nil {
		h.logger.WithError(err).Error("error listening for consumers")
	}
}

func (h *Handler) ListenForHeartbeatPublisher() {
	_, err := h.nc.Subscribe(h.publisherHeartbeatTopic, func(msg *nats.Msg) {
		h.logger.WithField("topic", h.publisherHeartbeatTopic).Trace("received heartbeat")
		if err := h.nc.Publish(h.consumerRegisterTopic, []byte(h.id)); err != nil {
			h.logger.WithField("topic", h.consumerRegisterTopic).Warn("could not publish to register topic")
		}
	})

	if err != nil {
		h.logger.WithError(err).Error("ListenForHeartbeatPublisher failed")
		return
	}
}

func (h *Handler) StartHeartbeatPublisher() {
	h.heartbeatTicker = time.NewTicker(h.heartbeatTickerTime)

	for {
		select {
		case <-h.heartbeatTicker.C:
			h.triggerHeartbeat()
		}
	}
}

func (h *Handler) GetConsumers() []string {
	h.rwlock.RLock()
	defer h.rwlock.RUnlock()

	return h.natsConsumer
}

func (h *Handler) addConsumer(s string) {
	h.rwlock.Lock()
	defer h.rwlock.Unlock()

	h.natsConsumer = append(h.natsConsumer, s)
	h.metrics.RecordConsumerRegistered(s, len(h.natsConsumer))
}

func (h *Handler) truncateConsumers() {
	h.rwlock.Lock()
	defer h.rwlock.Unlock()
	h.natsConsumer = []string{}
	h.metrics.ResetConsumerRegistered()
}

func (h *Handler) triggerHeartbeat() {
	h.truncateConsumers()
	h.logger.WithField("topic", h.publisherHeartbeatTopic).Trace("Heartbeat triggered")
	if err := h.nc.Publish(h.publisherHeartbeatTopic, []byte("ping")); err != nil {
		h.logger.WithError(err).Error("heartbeat trigger failure")
	}
}
