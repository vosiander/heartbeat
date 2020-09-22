package heartbeat

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const (
	PublisherTopic        = "peer-heartbeat"
	ConsumerRegisterTopic = "peer-consumer-register"
	CurrentConsumersTopic = "peer-consumers"
	TickerTime            = 30 * time.Second
	MetricsTimeout        = 2 * time.Second
)

type (
	Handler struct {
		id                      string
		nc                      *nats.Conn
		natsConsumer            []string
		rwlock                  *sync.RWMutex
		publisherHeartbeatTopic string
		consumerRegisterTopic   string
		currentConsumersTopic   string
		heartbeatTickers        []*time.Ticker
		heartbeatTickerTime     time.Duration
		metricsTimeout          time.Duration
		logger                  logrus.FieldLogger
		metrics                 MetricsHandler
	}

	MetricsHandler interface {
		RecordConsumerRegistered(ctx context.Context, id string, current int)
		ResetConsumerRegistered(ctx context.Context)
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
		currentConsumersTopic:   CurrentConsumersTopic,
		heartbeatTickerTime:     TickerTime,
		metricsTimeout:          MetricsTimeout,
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

func SetCurrentConsumersTopic(topic string) Option {
	return func(h *Handler) {
		h.currentConsumersTopic = topic
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

func SetMetricsTimeout(t time.Duration) Option {
	return func(h *Handler) {
		h.metricsTimeout = t
	}
}

func (h *Handler) Close() {
	for _, t := range h.heartbeatTickers {
		t.Stop()
	}
}

func (h *Handler) ListenForConsumers() {
	_, err := h.nc.Subscribe(h.consumerRegisterTopic, func(m *nats.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), h.metricsTimeout)
		defer cancel()
		s := string(m.Data)
		h.addConsumer(s)
		h.metrics.RecordConsumerRegistered(ctx, s, len(h.natsConsumer))

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
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(1000)

	h.heartbeatTickers = []*time.Ticker{
		h.schedule(h.triggerHeartbeat, h.heartbeatTickerTime),
		h.schedule(h.triggerConsumer, (time.Duration(n)*time.Millisecond)+time.Second),
	}
}

func (h *Handler) schedule(f func(), interval time.Duration) *time.Ticker {
	h.logger.WithField("interval", interval).Trace("enabling schedule")

	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			f()
		}
	}()
	return ticker
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
}

func (h *Handler) truncateConsumers() {
	h.rwlock.Lock()
	defer h.rwlock.Unlock()
	h.natsConsumer = []string{}

	ctx, cancel := context.WithTimeout(context.Background(), h.metricsTimeout)
	defer cancel()
	h.metrics.ResetConsumerRegistered(ctx)
}

func (h *Handler) triggerHeartbeat() {
	h.truncateConsumers()
	h.logger.WithField("topic", h.publisherHeartbeatTopic).Trace("Heartbeat triggered")
	if err := h.nc.Publish(h.publisherHeartbeatTopic, []byte("ping")); err != nil {
		h.logger.WithError(err).Error("heartbeat trigger failure")
	}
}

func (h *Handler) triggerConsumer() {
	currentConsumers := map[string][]string{"consumers": h.GetConsumers()}
	data, err := json.Marshal(currentConsumers)
	if err != nil {
		h.logger.WithError(err).Error("heartbeat current consumers failure")
	}
	if err := h.nc.Publish(h.currentConsumersTopic, data); err != nil {
		h.logger.WithError(err).Error("heartbeat current consumers failure")
	}
}
