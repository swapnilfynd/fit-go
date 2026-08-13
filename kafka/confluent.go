// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package kafka - confluent.go provides a real Kafka driver implementation using
// confluent-kafka-go (librdkafka wrapper). It satisfies the KafkaClient,
// KafkaProducer, and KafkaConsumer interfaces defined in kafka.go.
//
// This is the production driver for the fit Kafka integration, built on
// confluent-kafka-go (librdkafka) for performance and stability.
package kafka

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gofynd/fit-go/logging"
	"github.com/gofynd/fit-go/redact"
)

// ---------------------------------------------------------------------------
// Compile-time interface checks
// ---------------------------------------------------------------------------

var (
	_ KafkaClient           = (*ConfluentClient)(nil)
	_ KafkaProducer         = (*ConfluentProducer)(nil)
	_ KafkaConsumer         = (*ConfluentConsumer)(nil)
	_ KafkaBatchConsumerCtx = (*ConfluentConsumer)(nil)
)

// ---------------------------------------------------------------------------
// ConfluentClient
// ---------------------------------------------------------------------------

// ConfluentClient implements KafkaClient using the confluent-kafka-go library.
// It holds the resolved configuration and broker list, creating producers and
// consumers on demand.
type ConfluentClient struct {
	brokers []string
	fitCfg  *Config
	baseCfg *ckafka.ConfigMap
	logger  *logging.Logger

	mu     sync.Mutex
	closed bool
}

// NewConfluentClient creates a real Kafka client backed by confluent-kafka-go.
// It builds a ckafka.ConfigMap from the fit Config, including SASL, TLS,
// compression, and client ID settings. The client is ready to create producers
// and consumers but does not open any connections until Producer() or
// Consumer() is called.
func NewConfluentClient(cfg *Config) (*ConfluentClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kafka/confluent: config must not be nil")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka/confluent: no brokers configured")
	}

	baseCfg, err := buildConfluentConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka/confluent: failed to build config: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		l, lerr := logging.New(logging.Options{Level: "info"})
		if lerr != nil {
			return nil, fmt.Errorf("kafka/confluent: failed to create logger: %w", lerr)
		}
		logger = l
	}

	logger.Info("kafka/confluent: client created",
		"brokers", strings.Join(cfg.Brokers, ","),
		"clientId", cfg.ClientID,
		"compression", cfg.Compression,
	)

	return &ConfluentClient{
		brokers: cfg.Brokers,
		fitCfg:  cfg,
		baseCfg: baseCfg,
		logger:  logger,
	}, nil
}

// Producer creates a ConfluentProducer that satisfies the KafkaProducer
// interface. The producer is not connected until Connect() is called on it.
func (cc *ConfluentClient) Producer(config ProducerConfig) (KafkaProducer, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil, fmt.Errorf("kafka/confluent: client is closed")
	}
	if config.MetadataTimeout < 0 {
		return nil, fmt.Errorf("kafka/confluent: producer metadata timeout must not be negative")
	}
	if config.MetadataMaxAge < 0 {
		return nil, fmt.Errorf("kafka/confluent: producer metadata max age must not be negative")
	}

	// Clone the base config for producer-specific overrides.
	pCfg := cloneConfigMap(cc.baseCfg)

	configuredAcks := -1
	// Non-zero values historically meant "set". AcksSet adds the missing way
	// to explicitly request KafkaJS-compatible acks=0 at producer construction.
	if config.AcksSet || config.Acks != 0 {
		if err := setConfluentAcks(pCfg, config.Acks); err != nil {
			return nil, err
		}
		configuredAcks = config.Acks
	}

	if config.Compression != CompressionNone {
		_ = pCfg.SetKey("compression.type", mapCompressionToString(config.Compression))
	}

	if config.IdempotentProducer {
		_ = pCfg.SetKey("enable.idempotence", true)
		_ = pCfg.SetKey("acks", "all")
		_ = pCfg.SetKey("max.in.flight.requests.per.connection", 1)
		configuredAcks = -1
	}

	if config.Timeout > 0 {
		_ = pCfg.SetKey("request.timeout.ms", int(config.Timeout.Milliseconds()))
	}
	if config.DeliveryTimeout > 0 {
		_ = pCfg.SetKey("message.timeout.ms", int(config.DeliveryTimeout.Milliseconds()))
	}

	if config.MaxRetriesSet || config.MaxRetries > 0 {
		_ = pCfg.SetKey("message.send.max.retries", config.MaxRetries)
	}

	if config.RetryBackoff > 0 {
		_ = pCfg.SetKey("retry.backoff.ms", int(config.RetryBackoff.Milliseconds()))
	}
	if config.RetryBackoffMax > 0 {
		_ = pCfg.SetKey("retry.backoff.max.ms", int(config.RetryBackoffMax.Milliseconds()))
	}
	if config.ReconnectBackoff > 0 {
		_ = pCfg.SetKey("reconnect.backoff.ms", int(config.ReconnectBackoff.Milliseconds()))
	}
	if config.ReconnectBackoffMax > 0 {
		_ = pCfg.SetKey("reconnect.backoff.max.ms", int(config.ReconnectBackoffMax.Milliseconds()))
	}

	partitioner, err := confluentPartitioner(config.Partitioner)
	if err != nil {
		return nil, err
	}
	if partitioner != "" {
		_ = pCfg.SetKey("partitioner", partitioner)
	}
	if config.TraceHeaderPolicy != ProducerTraceHeadersInject && config.TraceHeaderPolicy != ProducerTraceHeadersPreserve {
		return nil, fmt.Errorf("kafka/confluent: unsupported producer trace header policy %d", config.TraceHeaderPolicy)
	}
	if config.ClosePolicy != ProducerCloseWaitForDelivery &&
		config.ClosePolicy != ProducerCloseKafkaJSDisconnect &&
		config.ClosePolicy != ProducerCloseKafkaJSAwaitDelivery {
		return nil, fmt.Errorf("kafka/confluent: unsupported producer close policy %d", config.ClosePolicy)
	}

	return &ConfluentProducer{
		configMap:         pCfg,
		logger:            cc.logger,
		brokers:           cc.brokers,
		configuredAcks:    configuredAcks,
		idempotent:        config.IdempotentProducer,
		traceHeaders:      config.TraceHeaderPolicy,
		closePolicy:       config.ClosePolicy,
		partitioner:       config.Partitioner,
		producers:         make(map[int]confluentProducerDriver),
		partitionCounters: make(map[string]uint32),
		metadataTimeout:   config.MetadataTimeout,
		metadataMaxAge:    config.MetadataMaxAge,
		metadataCache:     make(map[string]kafkaJSMetadataCacheEntry),
		metadataRefreshes: make(map[string]*kafkaJSMetadataRefresh),
	}, nil
}

// Consumer creates a ConfluentConsumer that satisfies the KafkaConsumer
// interface. The consumer is not connected until Connect() is called on it.
func (cc *ConfluentClient) Consumer(config ConsumerConfig) (KafkaConsumer, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil, fmt.Errorf("kafka/confluent: client is closed")
	}

	if config.GroupID == "" {
		return nil, fmt.Errorf("kafka/confluent: consumer group ID is required")
	}
	if config.Backend == ConsumerBackendKafkaJSCompatible {
		return newKafkaJSCompatibleConsumer(cc.brokers, cc.fitCfg, config, cc.logger)
	}
	if config.Backend != ConsumerBackendConfluent {
		return nil, fmt.Errorf("kafka: unsupported consumer backend %d", config.Backend)
	}

	// Clone the base config for consumer-specific overrides.
	cCfg := cloneConfigMap(cc.baseCfg)

	// The base config carries producer-only defaults (acks, compression) shared
	// with the producer path; drop them here so librdkafka doesn't log CONFWARN
	// about producer properties being ignored on a consumer instance.
	delete(*cCfg, "acks")
	delete(*cCfg, "compression.type")

	_ = cCfg.SetKey("group.id", config.GroupID)
	if config.PartitionAssignmentStrategy != "" {
		_ = cCfg.SetKey("partition.assignment.strategy", config.PartitionAssignmentStrategy)
	}

	if config.SessionTimeout > 0 {
		_ = cCfg.SetKey("session.timeout.ms", int(config.SessionTimeout.Milliseconds()))
	}
	if config.HeartbeatInterval > 0 {
		_ = cCfg.SetKey("heartbeat.interval.ms", int(config.HeartbeatInterval.Milliseconds()))
	}
	maxPollInterval := config.MaxPollInterval
	if maxPollInterval == 0 {
		maxPollInterval = config.RebalanceTimeout
	}
	if maxPollInterval > 0 {
		_ = cCfg.SetKey("max.poll.interval.ms", int(maxPollInterval.Milliseconds()))
	}
	if config.MaxBytesPerPartition > 0 {
		_ = cCfg.SetKey("max.partition.fetch.bytes", config.MaxBytesPerPartition)
	}
	if config.MinBytes > 0 {
		_ = cCfg.SetKey("fetch.min.bytes", config.MinBytes)
	}
	if config.MaxBytes > 0 {
		_ = cCfg.SetKey("fetch.max.bytes", config.MaxBytes)
	}
	if config.MaxWaitTime > 0 {
		_ = cCfg.SetKey("fetch.wait.max.ms", int(config.MaxWaitTime.Milliseconds()))
	}

	// Auto-commit settings.
	_ = cCfg.SetKey("enable.auto.commit", config.AutoCommit)
	// Resolve/store offsets only after a handler succeeds. librdkafka's default
	// stores on ReadMessage, which can commit work that is still running.
	_ = cCfg.SetKey("enable.auto.offset.store", false)
	if config.AutoCommitInterval > 0 {
		_ = cCfg.SetKey("auto.commit.interval.ms", int(config.AutoCommitInterval.Milliseconds()))
	}

	return &ConfluentConsumer{
		configMap: cCfg,
		groupID:   config.GroupID,
		config:    config,
		logger:    cc.logger,
	}, nil
}

// Close shuts down the confluent client and releases all resources.
func (cc *ConfluentClient) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil
	}
	cc.closed = true

	cc.logger.Info("kafka/confluent: client closed")
	return nil
}

// Ping performs the same broker-metadata class of check used by legacy
// KafkaJS admin.listTopics, without changing topics or consumer group state.
func (cc *ConfluentClient) Ping(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	admin, err := ckafka.NewAdminClient(cloneConfigMap(cc.baseCfg))
	if err != nil {
		return newKafkaHealthError("kafka/confluent: health admin client", err)
	}
	defer admin.Close()

	timeout := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return newKafkaHealthError("kafka/confluent: broker metadata", ctx.Err())
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	if _, err := admin.GetMetadata(nil, false, int(timeout.Milliseconds())); err != nil {
		return newKafkaHealthError("kafka/confluent: broker metadata", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ConfluentProducer
// ---------------------------------------------------------------------------

// ConfluentProducer implements KafkaProducer using confluent-kafka-go's
// Producer. It sends messages synchronously by waiting for delivery reports.
type ConfluentProducer struct {
	configMap      *ckafka.ConfigMap
	logger         *logging.Logger
	brokers        []string
	configuredAcks int
	idempotent     bool
	traceHeaders   ProducerTraceHeaderPolicy
	closePolicy    ProducerClosePolicy
	partitioner    ProducerPartitioner
	newProducer    func(*ckafka.ConfigMap) (confluentProducerDriver, error)

	mu        sync.Mutex
	inFlight  sync.WaitGroup
	producer  confluentProducerDriver
	producers map[int]confluentProducerDriver
	closed    bool
	closeDone chan struct{}
	closeErr  error

	closeTimeout   time.Duration
	pendingReports atomic.Int64

	partitionMu       sync.Mutex
	partitionCounters map[string]uint32
	partitionSeed     func() (uint32, error)

	metadataMu        sync.Mutex
	metadataTimeout   time.Duration
	metadataMaxAge    time.Duration
	metadataNow       func() time.Time
	metadataCache     map[string]kafkaJSMetadataCacheEntry
	metadataRefreshes map[string]*kafkaJSMetadataRefresh
}

type kafkaJSMetadataCacheEntry struct {
	partitions []ckafka.PartitionMetadata
	expiresAt  time.Time
}

type kafkaJSMetadataRefresh struct {
	done       chan struct{}
	partitions []ckafka.PartitionMetadata
	err        error
}

// confluentProducerDriver is the narrow librdkafka surface used by the fit
// producer. Keeping it internal makes delivery and shutdown behavior testable
// without a Kafka broker while *ckafka.Producer remains the production driver.
type confluentProducerDriver interface {
	Produce(*ckafka.Message, chan ckafka.Event) error
	Flush(timeoutMs int) int
	Close()
}

type confluentProducerMetadataDriver interface {
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*ckafka.Metadata, error)
}

// Connect establishes the confluent Producer connection to the brokers.
func (cp *ConfluentProducer) Connect() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return fmt.Errorf("kafka/confluent: producer is closed")
	}
	if cp.producer != nil {
		return nil
	}

	producer, err := cp.createProducer(cp.configMap)
	if err != nil {
		return fmt.Errorf("kafka/confluent: producer connect failed: %w", err)
	}

	cp.producer = producer
	if cp.producers == nil {
		cp.producers = make(map[int]confluentProducerDriver)
	}
	cp.producers[cp.configuredAcks] = producer
	cp.logger.Info("kafka/confluent: producer connected",
		"brokers", strings.Join(cp.brokers, ","),
	)
	return nil
}

const defaultProducerCloseTimeout = 15 * time.Second

// Produce sends messages to a single topic. Raw calls retain KafkaJS-like
// automatic tracing by adopting FIT's active goroutine context when one exists.
// ProducerTraceHeadersPreserve leaves record headers untouched while retaining
// the producer spans.
func (cp *ConfluentProducer) Produce(topic string, messages []Message, acks int) error {
	ctx := automaticProducerContext()
	return produceTopicMessagesWithTracePolicy(
		ctx,
		[]TopicMessages{{Topic: topic, Messages: messages}},
		acks,
		cp.traceHeaders,
		func(traced []TopicMessages, tracedAcks int) error {
			_, err := cp.produceWithMetadata(ctx, topic, traced[0].Messages, tracedAcks)
			return err
		},
	)
}

// ProduceWithMetadata sends messages to a single topic and returns KafkaJS-style
// broker delivery metadata grouped by topic/partition. It is intentionally a
// concrete ConfluentProducer capability rather than a KafkaProducer interface
// requirement, so existing drivers and callers that only need Produce remain
// unchanged.
func (cp *ConfluentProducer) ProduceWithMetadata(topic string, messages []Message, acks int) ([]RecordMetadata, error) {
	ctx := automaticProducerContext()
	topicMessages := []TopicMessages{{Topic: topic, Messages: messages}}
	spans := startProducerMessageSpansWithPolicy(ctx, topicMessages, cp.traceHeaders)
	metadata, err := cp.produceWithMetadata(ctx, topic, topicMessages[0].Messages, acks)
	endProducerMessageSpans(spans, err)
	return metadata, err
}

func (cp *ConfluentProducer) produceWithMetadata(ctx context.Context, topic string, messages []Message, acks int) ([]RecordMetadata, error) {
	producer, done, err := cp.beginProduce(acks)
	if err != nil {
		return nil, err
	}

	brokerMessages, err := cp.buildBrokerMessages(ctx, producer, topic, messages)
	if err != nil {
		done()
		return nil, err
	}
	deliveries, err := cp.produceAndDrain(ctx, producer, brokerMessages, done)
	if err != nil {
		cp.invalidateKafkaJSMetadataOnError(err, topic)
		return nil, fmt.Errorf("kafka/confluent: produce to %s failed: %w", topic, err)
	}

	metadata := make([]RecordMetadata, 0, len(messages))
	metadataByPartition := make(map[string]int, len(messages))
	for _, m := range deliveries {
		md := mapConfluentToRecordMetadata(m)
		key := fmt.Sprintf("%s:%d", md.TopicName, md.Partition)
		if idx, ok := metadataByPartition[key]; ok {
			if md.Offset < metadata[idx].Offset {
				metadata[idx].Offset = md.Offset
				metadata[idx].BaseOffset = md.BaseOffset
			}
			continue
		}
		metadataByPartition[key] = len(metadata)
		metadata = append(metadata, md)
	}

	return metadata, nil
}

// ProduceBatch sends messages to multiple topics in one call.
func (cp *ConfluentProducer) ProduceBatch(topicMessages []TopicMessages, acks int) error {
	ctx := automaticProducerContext()
	return produceTopicMessagesWithTracePolicy(ctx, topicMessages, acks, cp.traceHeaders, func(traced []TopicMessages, tracedAcks int) error {
		return cp.produceBatch(ctx, traced, tracedAcks)
	})
}

func (cp *ConfluentProducer) produceBatch(ctx context.Context, topicMessages []TopicMessages, acks int) error {
	producer, done, err := cp.beginProduce(acks)
	if err != nil {
		return err
	}

	totalMessages := 0
	for _, tm := range topicMessages {
		totalMessages += len(tm.Messages)
	}

	if totalMessages == 0 {
		done()
		return nil
	}

	brokerMessages := make([]*ckafka.Message, 0, totalMessages)
	for _, tm := range topicMessages {
		messages, buildErr := cp.buildBrokerMessages(ctx, producer, tm.Topic, tm.Messages)
		if buildErr != nil {
			done()
			return buildErr
		}
		brokerMessages = append(brokerMessages, messages...)
	}
	if _, err := cp.produceAndDrain(ctx, producer, brokerMessages, done); err != nil {
		topics := make([]string, 0, len(topicMessages))
		for _, topicGroup := range topicMessages {
			topics = append(topics, topicGroup.Topic)
		}
		cp.invalidateKafkaJSMetadataOnError(err, topics...)
		return fmt.Errorf("kafka/confluent: batch produce failed: %w", err)
	}

	return nil
}

// ProduceCtx is the canonical producer path. It creates one producer span per
// message, matching KafkaJS instrumentation, and injects propagation headers
// unless ProducerTraceHeadersPreserve was selected. It then performs the same
// raw broker operation with the caller's acks value.
func (cp *ConfluentProducer) ProduceCtx(ctx context.Context, topic string, messages []Message, acks int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return produceTopicMessagesWithTracePolicy(
		ctx,
		[]TopicMessages{{Topic: topic, Messages: messages}},
		acks,
		cp.traceHeaders,
		func(traced []TopicMessages, tracedAcks int) error {
			_, err := cp.produceWithMetadata(ctx, topic, traced[0].Messages, tracedAcks)
			return err
		},
	)
}

// ProduceCtxWithMetadata is ProduceWithMetadata with a producer span and the
// configured per-message trace-header policy.
func (cp *ConfluentProducer) ProduceCtxWithMetadata(ctx context.Context, topic string, messages []Message, acks int) ([]RecordMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	topicMessages := []TopicMessages{{Topic: topic, Messages: messages}}
	spans := startProducerMessageSpansWithPolicy(ctx, topicMessages, cp.traceHeaders)
	md, err := cp.produceWithMetadata(ctx, topic, topicMessages[0].Messages, acks)
	endProducerMessageSpans(spans, err)
	return md, err
}

// ProduceBatchCtx creates one correctly-labelled producer span per message across
// every topic, injects from each message's span, and preserves the raw batch call's
// message ordering, acks and delivery error.
func (cp *ConfluentProducer) ProduceBatchCtx(ctx context.Context, topicMessages []TopicMessages, acks int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return produceTopicMessagesWithTracePolicy(ctx, topicMessages, acks, cp.traceHeaders, func(traced []TopicMessages, tracedAcks int) error {
		return cp.produceBatch(ctx, traced, tracedAcks)
	})
}

// Close stops admission immediately and bounds how long the caller waits for
// accepted delivery reports and policy-specific driver shutdown. The default
// policy includes Flush; KafkaJS compatibility policies define their narrower
// disconnect boundaries explicitly. If the deadline is reached, the same
// ordered shutdown continues in the background; a driver is never closed while
// an accepted delivery report is still being drained.
func (cp *ConfluentProducer) Close() error {
	cp.mu.Lock()
	if cp.closeDone != nil {
		done := cp.closeDone
		cp.mu.Unlock()
		<-done
		cp.mu.Lock()
		err := cp.closeErr
		cp.mu.Unlock()
		return err
	}
	cp.closed = true
	cp.closeDone = make(chan struct{})
	done := cp.closeDone
	timeout := cp.closeTimeout
	if timeout <= 0 {
		timeout = defaultProducerCloseTimeout
	}

	// Flush every acknowledgement-specific producer exactly once. Per-call acks
	// require separate librdkafka instances because acks is not a request option.
	unique := make(map[confluentProducerDriver]struct{}, len(cp.producers)+1)
	if cp.producer != nil {
		unique[cp.producer] = struct{}{}
	}
	for _, producer := range cp.producers {
		if producer != nil {
			unique[producer] = struct{}{}
		}
	}
	if cp.closePolicy == ProducerCloseKafkaJSDisconnect {
		cp.closeErr = nil
		close(done)
		cp.mu.Unlock()
		go cp.finishKafkaJSDisconnect(unique, timeout)
		cp.logger.Info("kafka/confluent: producer disconnected")
		return nil
	}
	cp.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		if cp.closePolicy == ProducerCloseKafkaJSAwaitDelivery {
			result <- cp.finishKafkaJSAwaitedDelivery(unique)
			return
		}
		result <- cp.finishClose(unique, timeout)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var closeErr error
	select {
	case closeErr = <-result:
	case <-timer.C:
		closeErr = fmt.Errorf(
			"kafka/confluent: producer close timed out after %s with %d accepted delivery report(s) outstanding; shutdown continues in background",
			timeout,
			cp.pendingReports.Load(),
		)
	}

	cp.mu.Lock()
	cp.closeErr = closeErr
	close(done)
	cp.mu.Unlock()

	if closeErr != nil {
		cp.logger.Warn("kafka/confluent: producer close incomplete", "error", redact.ErrorMessage(closeErr))
	} else {
		cp.logger.Info("kafka/confluent: producer closed")
	}
	return closeErr
}

// finishKafkaJSAwaitedDelivery models an awaited KafkaJS disconnect without
// making an additional Flush call the success oracle. Every accepted fit-go
// delivery already owns a drainer and keeps inFlight non-zero until its report
// is consumed. Waiting on that boundary therefore proves there is no report
// left for Flush to dispatch before driver Close invalidates resources.
//
// Close applies the external time bound. If it expires, this function remains
// in the background and closes the drivers only after the outstanding drainers
// finish, so timeout reporting never introduces a close-versus-delivery race.
func (cp *ConfluentProducer) finishKafkaJSAwaitedDelivery(
	producers map[confluentProducerDriver]struct{},
) error {
	cp.inFlight.Wait()
	remaining := cp.pendingReports.Load()
	for producer := range producers {
		producer.Close()
	}

	cp.mu.Lock()
	cp.producer = nil
	cp.producers = nil
	cp.mu.Unlock()
	if remaining != 0 {
		return fmt.Errorf(
			"kafka/confluent: producer close left %d accepted delivery report(s) unresolved after drain",
			remaining,
		)
	}
	return nil
}

func (cp *ConfluentProducer) finishKafkaJSDisconnect(
	producers map[confluentProducerDriver]struct{},
	timeout time.Duration,
) {
	if err := cp.finishClose(producers, timeout); err != nil {
		cp.logger.Warn("kafka/confluent: producer background disconnect incomplete", "error", redact.ErrorMessage(err))
	}
}

func (cp *ConfluentProducer) finishClose(producers map[confluentProducerDriver]struct{}, timeout time.Duration) error {
	type flushResult struct{ remaining int }
	flushResults := make(chan flushResult, len(producers))
	timeoutMs := int(timeout.Milliseconds())
	if timeoutMs < 1 {
		timeoutMs = 1
	}
	for producer := range producers {
		go func(driver confluentProducerDriver) {
			flushResults <- flushResult{remaining: driver.Flush(timeoutMs)}
		}(producer)
	}

	remaining := 0
	for range producers {
		remaining += (<-flushResults).remaining
	}

	// Flush dispatches delivery reports; wait until their drainers have consumed
	// every accepted result before allowing driver Close to invalidate resources.
	cp.inFlight.Wait()
	for producer := range producers {
		producer.Close()
	}

	cp.mu.Lock()
	cp.producer = nil
	cp.producers = nil
	cp.mu.Unlock()

	if remaining > 0 {
		return fmt.Errorf("kafka/confluent: producer close left %d message(s) undelivered after flush", remaining)
	}
	return nil
}

func setConfluentAcks(config *ckafka.ConfigMap, acks int) error {
	if config == nil {
		return fmt.Errorf("kafka/confluent: producer config is nil")
	}
	var value any
	switch acks {
	case -1:
		value = "all"
	case 0, 1:
		value = fmt.Sprintf("%d", acks)
	default:
		return fmt.Errorf("kafka/confluent: unsupported acks value %d (want -1, 0, or 1)", acks)
	}
	if err := config.SetKey("acks", value); err != nil {
		return fmt.Errorf("kafka/confluent: configure acks: %w", err)
	}
	return nil
}

// producerForAcks honors KafkaJS's per-send acks contract. librdkafka exposes
// acks only as producer configuration, so each distinct value is backed by a
// cached producer with otherwise identical connection/security settings.
func (cp *ConfluentProducer) beginProduce(acks int) (confluentProducerDriver, func(), error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	producer, err := cp.producerForAcksLocked(acks)
	if err != nil {
		return nil, nil, err
	}
	cp.inFlight.Add(1)
	return producer, cp.inFlight.Done, nil
}

func (cp *ConfluentProducer) producerForAcksLocked(acks int) (confluentProducerDriver, error) {
	if cp.closed {
		return nil, fmt.Errorf("kafka/confluent: producer is closed")
	}
	if cp.producer == nil {
		return nil, fmt.Errorf("kafka/confluent: producer not connected")
	}
	if cp.idempotent && acks != -1 {
		return nil, fmt.Errorf("kafka/confluent: idempotent producer requires acks=-1")
	}
	if existing := cp.producers[acks]; existing != nil {
		return existing, nil
	}

	config := cloneConfigMap(cp.configMap)
	if err := setConfluentAcks(config, acks); err != nil {
		return nil, err
	}
	producer, err := cp.createProducer(config)
	if err != nil {
		return nil, fmt.Errorf("kafka/confluent: producer for acks=%d failed: %w", acks, err)
	}
	if cp.producers == nil {
		cp.producers = make(map[int]confluentProducerDriver)
	}
	cp.producers[acks] = producer
	return producer, nil
}

func (cp *ConfluentProducer) createProducer(config *ckafka.ConfigMap) (confluentProducerDriver, error) {
	if cp.newProducer != nil {
		return cp.newProducer(config)
	}
	return ckafka.NewProducer(config)
}

// produceAndDrain enqueues records and consumes exactly one delivery event for
// every record accepted by librdkafka. Cancellation returns control to the caller
// immediately, while the drainer remains responsible for accepted reports and
// keeps the producer in-flight until they arrive. The librdkafka-owned delivery
// channel is deliberately never closed by fit-go.
func (cp *ConfluentProducer) produceAndDrain(
	ctx context.Context,
	producer confluentProducerDriver,
	messages []*ckafka.Message,
	done func(),
) ([]*ckafka.Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(messages) == 0 {
		done()
		return nil, nil
	}

	deliveryChan := make(chan ckafka.Event, len(messages))
	accepted := 0
	var firstErr error
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			firstErr = err
			break
		}
		if err := producer.Produce(message, deliveryChan); err != nil {
			firstErr = fmt.Errorf("enqueue failed: %w", err)
			break
		}
		accepted++
		cp.pendingReports.Add(1)
	}

	if accepted == 0 {
		done()
		return nil, firstErr
	}

	type drainResult struct {
		deliveries []*ckafka.Message
		err        error
	}
	result := make(chan drainResult, 1)
	go func(initialErr error) {
		defer done()
		deliveries := make([]*ckafka.Message, 0, accepted)
		drainErr := initialErr
		for i := 0; i < accepted; i++ {
			event, ok := <-deliveryChan
			if !ok {
				if drainErr == nil {
					drainErr = fmt.Errorf("delivery channel closed with %d report(s) outstanding", accepted-i)
				}
				break
			}
			cp.pendingReports.Add(-1)
			message, ok := event.(*ckafka.Message)
			if !ok || message == nil {
				if drainErr == nil {
					drainErr = fmt.Errorf("unexpected delivery event %T", event)
				}
				continue
			}
			deliveries = append(deliveries, message)
			if message.TopicPartition.Error != nil && drainErr == nil {
				drainErr = fmt.Errorf("delivery failed: %w", message.TopicPartition.Error)
			}
		}
		result <- drainResult{deliveries: deliveries, err: drainErr}
	}(firstErr)

	// Prefer cancellation when it was already observable before delivery
	// completion; this makes a canceled context deterministic even when the last
	// report races the select below.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case drained := <-result:
		return drained.deliveries, drained.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// ConfluentConsumer
// ---------------------------------------------------------------------------

// ConfluentConsumer implements KafkaConsumer using confluent-kafka-go's
// Consumer. It manages topic subscriptions, message dispatch, and graceful
// shutdown.
type ConfluentConsumer struct {
	configMap *ckafka.ConfigMap
	groupID   string
	config    ConsumerConfig
	logger    *logging.Logger

	mu        sync.Mutex
	consumer  confluentConsumerDriver
	topics    []string
	cancelFn  context.CancelFunc
	runDone   chan struct{}
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

// confluentConsumerDriver is the message-loop subset of *ckafka.Consumer.
// Connect still creates the real driver and supplies its rebalance callback;
// the narrow interface keeps run-option and commit semantics broker-free in
// unit tests.
type confluentConsumerDriver interface {
	ReadMessage(timeout time.Duration) (*ckafka.Message, error)
	CommitMessage(message *ckafka.Message) ([]ckafka.TopicPartition, error)
	CommitOffsets(offsets []ckafka.TopicPartition) ([]ckafka.TopicPartition, error)
	StoreMessage(message *ckafka.Message) ([]ckafka.TopicPartition, error)
	Close() error
}

// Connect subscribes to the given topics by creating a confluent Consumer.
// Messages are not consumed until Consume() or ConsumeBatch() is called.
func (cc *ConfluentConsumer) Connect(topics []TopicConfig) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return fmt.Errorf("kafka/confluent: consumer is closed")
	}
	if cc.consumer != nil {
		return nil
	}

	// Set initial offset based on FromBeginning.
	if len(topics) > 0 && topics[0].FromBeginning {
		_ = cc.configMap.SetKey("auto.offset.reset", "earliest")
	} else {
		_ = cc.configMap.SetKey("auto.offset.reset", "latest")
	}

	consumer, err := ckafka.NewConsumer(cc.configMap)
	if err != nil {
		return fmt.Errorf("kafka/confluent: consumer group connect failed: %w", err)
	}

	// Extract topic names for subscription.
	names := make([]string, len(topics))
	for i, t := range topics {
		names[i] = t.Topic
	}

	// Opt-in only (default off = legacy fit.js subscribe-only behaviour): when
	// AutoCreateTopics is set, best-effort create any missing topics before
	// subscribing. Never blocks startup — failures are logged and we fall through
	// to subscribe.
	if cc.config.AutoCreateTopics {
		cc.ensureTopics(names)
	}

	// Subscribe with a rebalance callback for partition-assignment visibility and
	// optional app hooks. Once a RebalanceCb is supplied we own assign/unassign
	// (eager protocol), which the callback handles.
	if err := consumer.SubscribeTopics(names, cc.rebalanceCb); err != nil {
		consumer.Close()
		return fmt.Errorf("kafka/confluent: subscribe failed: %w", err)
	}

	cc.consumer = consumer
	cc.topics = names

	cc.logger.Info("kafka/confluent: consumer connected",
		"groupId", cc.groupID,
		"topics", strings.Join(names, ","),
	)
	return nil
}

// ensureTopics best-effort creates any of the given topics that don't yet exist,
// using the broker's default partitions/replication. All failures are logged and
// swallowed — topic creation must never block consumer startup.
func (cc *ConfluentConsumer) ensureTopics(names []string) {
	admin, err := ckafka.NewAdminClient(cloneConfigMap(cc.configMap))
	if err != nil {
		cc.logger.Warn("kafka/confluent: topic auto-create skipped (admin client)", "error", redact.ErrorMessage(err))
		return
	}
	defer admin.Close()

	specs := make([]ckafka.TopicSpecification, len(names))
	for i, n := range names {
		// -1 => use the broker's default num.partitions / replication factor.
		specs[i] = ckafka.TopicSpecification{Topic: n, NumPartitions: -1, ReplicationFactor: -1}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := admin.CreateTopics(ctx, specs)
	if err != nil {
		cc.logger.Warn("kafka/confluent: topic auto-create failed", "error", redact.ErrorMessage(err))
		return
	}
	for _, r := range results {
		switch r.Error.Code() {
		case ckafka.ErrNoError:
			cc.logger.Info("kafka/confluent: topic created", "topic", r.Topic)
		case ckafka.ErrTopicAlreadyExists:
			// already present — nothing to do
		default:
			cc.logger.Warn("kafka/confluent: topic create result", "topic", r.Topic, "error", r.Error.String())
		}
	}
}

// rebalanceCb drives partition assignment/revocation for the subscription.
// Supplying a RebalanceCb means we own assign/unassign (eager protocol). It logs
// the partition set and invokes the optional ConsumerConfig hooks.
func (cc *ConfluentConsumer) rebalanceCb(consumer *ckafka.Consumer, event ckafka.Event) error {
	// Use incremental (un)assign under the cooperative-sticky protocol and the
	// wholesale variants under the eager protocol (librdkafka default). Mixing
	// them corrupts the assignment, so branch on the negotiated protocol rather
	// than assuming eager.
	cooperative := consumer.GetRebalanceProtocol() == "COOPERATIVE"
	switch e := event.(type) {
	case ckafka.AssignedPartitions:
		var err error
		if cooperative {
			err = consumer.IncrementalAssign(e.Partitions)
		} else {
			err = consumer.Assign(e.Partitions)
		}
		if err != nil {
			// Surface it, don't swallow: returning nil tells librdkafka the rebalance
			// succeeded with no partitions assigned — a silent zombie consumer that
			// polls forever and reads nothing.
			cc.logger.Error("kafka/confluent: partition assign failed", "groupId", cc.groupID, "error", redact.ErrorMessage(err))
			return err
		}
		cc.logger.Info("kafka/confluent: partitions assigned",
			"groupId", cc.groupID, "partitions", formatPartitions(e.Partitions))
		if cc.config.OnPartitionsAssigned != nil {
			cc.config.OnPartitionsAssigned(toPartitionAssignments(e.Partitions))
		}
	case ckafka.RevokedPartitions:
		cc.logger.Info("kafka/confluent: partitions revoked",
			"groupId", cc.groupID, "partitions", formatPartitions(e.Partitions))
		if cc.config.OnPartitionsRevoked != nil {
			cc.config.OnPartitionsRevoked(toPartitionAssignments(e.Partitions))
		}
		var err error
		if cooperative {
			err = consumer.IncrementalUnassign(e.Partitions)
		} else {
			err = consumer.Unassign()
		}
		if err != nil {
			cc.logger.Error("kafka/confluent: partition unassign failed", "groupId", cc.groupID, "error", redact.ErrorMessage(err))
			return err
		}
	}
	return nil
}

// formatPartitions renders partitions as "topic[partition]" for logs.
func formatPartitions(parts []ckafka.TopicPartition) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := ""
		if p.Topic != nil {
			t = *p.Topic
		}
		out = append(out, fmt.Sprintf("%s[%d]", t, p.Partition))
	}
	return out
}

// toPartitionAssignments maps confluent partitions to the public type.
func toPartitionAssignments(parts []ckafka.TopicPartition) []PartitionAssignment {
	out := make([]PartitionAssignment, 0, len(parts))
	for _, p := range parts {
		t := ""
		if p.Topic != nil {
			t = *p.Topic
		}
		out = append(out, PartitionAssignment{Topic: t, Partition: p.Partition})
	}
	return out
}

type confluentMessageHandler func(context.Context, MessagePayload) error
type confluentBatchHandler func(context.Context, BatchPayload) error

type exactOffsetCommitError struct{ err error }

func (e *exactOffsetCommitError) Error() string { return e.err.Error() }
func (e *exactOffsetCommitError) Unwrap() error { return e.err }

// Consume processes messages with automatic KafkaJS-like consumer tracing. The
// span context is active for same-goroutine FIT helpers even though the raw
// handler signature remains source compatible.
func (cc *ConfluentConsumer) Consume(handler MessageHandler, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/confluent: message handler is nil")
	}
	return cc.consumeMessages(func(ctx context.Context, payload MessagePayload) error {
		return runTracedMessageHandler(ctx, payload, func(_ context.Context, traced MessagePayload) error {
			return handler(traced)
		})
	}, opts)
}

func (cc *ConfluentConsumer) ConsumeCtx(handler MessageHandlerCtx, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/confluent: message handler is nil")
	}
	return cc.consumeMessages(func(ctx context.Context, payload MessagePayload) error {
		return runTracedMessageHandler(ctx, payload, handler)
	}, opts)
}

func (cc *ConfluentConsumer) consumeMessages(handler confluentMessageHandler, opts ConsumerOptions) error {
	isAutoCommit, pollTimeout, concurrency, err := validateConfluentConsumerOptions(cc.config.AutoCommit, opts, 100*time.Millisecond)
	if err != nil {
		return err
	}
	consumer, ctx, finish, err := cc.beginConsumeRun()
	if err != nil {
		return err
	}
	defer finish()

	waveSize := opts.MaxRecords
	if waveSize <= 0 {
		waveSize = concurrency
	}
	if waveSize < 1 {
		waveSize = 1
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		first, readErr := consumer.ReadMessage(pollTimeout)
		if readErr != nil {
			if isConfluentPollTimeout(readErr) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka/confluent: consume error: %w", readErr)
		}

		messages := []*ckafka.Message{first}
		var deferredReadErr error
		for len(messages) < waveSize && ctx.Err() == nil {
			message, err := consumer.ReadMessage(0)
			if err != nil {
				if isConfluentPollTimeout(err) {
					break
				}
				deferredReadErr = err
				break
			}
			messages = append(messages, message)
		}
		if ctx.Err() != nil {
			return nil
		}
		groups := groupConfluentBatchMessages(messages)
		if err := runConfluentPartitionGroups(ctx, groups, concurrency, func(groupCtx context.Context, group confluentBatchGroup) error {
			return cc.processMessageGroup(groupCtx, consumer, group, handler, isAutoCommit, opts)
		}); err != nil {
			return err
		}
		if deferredReadErr != nil {
			return fmt.Errorf("kafka/confluent: consume error: %w", deferredReadErr)
		}
	}
}

func (cc *ConfluentConsumer) processMessageGroup(
	ctx context.Context,
	consumer confluentConsumerDriver,
	group confluentBatchGroup,
	handler confluentMessageHandler,
	isAutoCommit bool,
	opts ConsumerOptions,
) error {
	for i, message := range group.messages {
		if ctx.Err() != nil {
			return nil
		}
		payload := group.payload.Messages[i]
		if !isAutoCommit && opts.CommitBeforeHandler {
			if _, err := consumer.CommitMessage(message); err != nil {
				cc.logMessageFailure("kafka/confluent: pre-handler commit failed", message, err)
				return fmt.Errorf("kafka/confluent: pre-handler commit failed: %w", err)
			}
		}

		handlerErr := handler(ctx, payload)
		if opts.OffsetFinalizer != nil {
			commitCalled := false
			commitExact := func(exact int64) error {
				if commitCalled {
					return fmt.Errorf("kafka/confluent: exact offset commit callback called more than once")
				}
				commitCalled = true
				offset := message.TopicPartition
				offset.Offset = ckafka.Offset(exact)
				if opts.NullOffsetCommitMetadata {
					offset.Metadata = nil
				}
				if _, err := consumer.CommitOffsets([]ckafka.TopicPartition{offset}); err != nil {
					return &exactOffsetCommitError{err: err}
				}
				return nil
			}
			finalizerErr := opts.OffsetFinalizer(ctx, payload, handlerErr, commitExact)
			if finalizerErr != nil {
				var commitErr *exactOffsetCommitError
				if errors.As(finalizerErr, &commitErr) {
					cc.logMessageFailure("kafka/confluent: exact offset commit failed", message, finalizerErr)
					return fmt.Errorf("kafka/confluent: exact offset commit failed: %w", finalizerErr)
				}
				if handlerErr != nil && errors.Is(finalizerErr, handlerErr) {
					cc.logMessageFailure("kafka/confluent: message handler error", message, finalizerErr)
					return fmt.Errorf("kafka/confluent: message handler failed: %w", finalizerErr)
				}
				cc.logMessageFailure("kafka/confluent: offset finalizer failed", message, finalizerErr)
				return fmt.Errorf("kafka/confluent: offset finalizer failed: %w", finalizerErr)
			}
			if handlerErr == nil && opts.ResolveAfterSuccessfulFinalizer {
				var err error
				if opts.NullOffsetCommitMetadata {
					resolved := message.TopicPartition
					resolved.Offset++
					resolved.Metadata = nil
					if _, commitErr := consumer.CommitOffsets([]ckafka.TopicPartition{resolved}); commitErr != nil {
						err = fmt.Errorf("kafka/confluent: post-handler commit failed: %w", commitErr)
					}
				} else {
					err = cc.resolveMessageOffset(consumer, message, isAutoCommit, false, false)
				}
				if err != nil {
					cc.logMessageFailure("kafka/confluent: post-finalizer offset resolution failed", message, err)
					return err
				}
			}
			continue
		}

		if handlerErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			cc.logMessageFailure("kafka/confluent: message handler error", message, handlerErr)
			return fmt.Errorf("kafka/confluent: message handler failed: %w", handlerErr)
		}

		if err := cc.resolveMessageOffset(consumer, message, isAutoCommit, opts.CommitBeforeHandler, opts.NullOffsetCommitMetadata); err != nil {
			cc.logMessageFailure("kafka/confluent: post-handler offset resolution failed", message, err)
			return err
		}
	}
	return nil
}

func (cc *ConfluentConsumer) ConsumeBatch(handler BatchHandler, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/confluent: batch handler is nil")
	}
	if opts.OffsetFinalizer != nil {
		return fmt.Errorf("kafka/confluent: OffsetFinalizer is supported only for message consumption")
	}
	return cc.consumeBatches(func(ctx context.Context, payload BatchPayload) error {
		return runTracedBatchHandler(ctx, payload, func(_ context.Context, traced BatchPayload) error {
			return handler(traced)
		})
	}, opts)
}

func (cc *ConfluentConsumer) ConsumeBatchCtx(handler BatchHandlerCtx, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/confluent: batch handler is nil")
	}
	if opts.OffsetFinalizer != nil {
		return fmt.Errorf("kafka/confluent: OffsetFinalizer is supported only for message consumption")
	}
	return cc.consumeBatches(func(ctx context.Context, payload BatchPayload) error {
		return runTracedBatchHandler(ctx, payload, handler)
	}, opts)
}

func (cc *ConfluentConsumer) consumeBatches(handler confluentBatchHandler, opts ConsumerOptions) error {
	isAutoCommit, batchTimeout, concurrency, err := validateConfluentConsumerOptions(cc.config.AutoCommit, opts, time.Second)
	if err != nil {
		return err
	}
	consumer, ctx, finish, err := cc.beginConsumeRun()
	if err != nil {
		return err
	}
	defer finish()

	batchSize := opts.MaxRecords
	if batchSize <= 0 {
		batchSize = 100
	}
	for {
		if ctx.Err() != nil {
			return nil
		}

		messages := make([]*ckafka.Message, 0, batchSize)
		deadline := time.Now().Add(batchTimeout)
		for len(messages) < batchSize && time.Now().Before(deadline) && ctx.Err() == nil {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			message, err := consumer.ReadMessage(remaining)
			if err != nil {
				if isConfluentPollTimeout(err) {
					break
				}
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("kafka/confluent: consume batch error: %w", err)
			}
			messages = append(messages, message)
		}

		if ctx.Err() != nil {
			return nil
		}
		if len(messages) == 0 {
			continue
		}
		groups := groupConfluentBatchMessages(messages)
		if err := runConfluentPartitionGroups(ctx, groups, concurrency, func(groupCtx context.Context, group confluentBatchGroup) error {
			return cc.processBatchGroup(groupCtx, consumer, group, handler, isAutoCommit, opts)
		}); err != nil {
			return err
		}
	}
}

func (cc *ConfluentConsumer) processBatchGroup(
	ctx context.Context,
	consumer confluentConsumerDriver,
	group confluentBatchGroup,
	handler confluentBatchHandler,
	isAutoCommit bool,
	opts ConsumerOptions,
) error {
	if !isAutoCommit && opts.CommitBeforeHandler {
		if _, err := consumer.CommitMessage(group.lastMessage); err != nil {
			cc.logBatchFailure("kafka/confluent: pre-handler batch commit failed", group.payload, err)
			return fmt.Errorf("kafka/confluent: pre-handler batch commit failed: %w", err)
		}
	}
	if err := handler(ctx, group.payload); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		cc.logBatchFailure("kafka/confluent: batch handler error", group.payload, err)
		return err
	}
	if err := cc.resolveMessageOffset(consumer, group.lastMessage, isAutoCommit, opts.CommitBeforeHandler, opts.NullOffsetCommitMetadata); err != nil {
		cc.logBatchFailure("kafka/confluent: post-handler batch offset resolution failed", group.payload, err)
		return err
	}
	return nil
}

func (cc *ConfluentConsumer) resolveMessageOffset(
	consumer confluentConsumerDriver,
	message *ckafka.Message,
	isAutoCommit bool,
	committedBeforeHandler bool,
	nullMetadata bool,
) error {
	if !isAutoCommit && committedBeforeHandler {
		return nil
	}
	if isAutoCommit && cc.config.AutoCommit {
		if _, err := consumer.StoreMessage(message); err != nil {
			return fmt.Errorf("kafka/confluent: post-handler offset store failed: %w", err)
		}
		return nil
	}
	if nullMetadata {
		resolved := message.TopicPartition
		resolved.Offset++
		resolved.Metadata = nil
		if _, err := consumer.CommitOffsets([]ckafka.TopicPartition{resolved}); err != nil {
			return fmt.Errorf("kafka/confluent: post-handler commit failed: %w", err)
		}
		return nil
	}
	if _, err := consumer.CommitMessage(message); err != nil {
		return fmt.Errorf("kafka/confluent: post-handler commit failed: %w", err)
	}
	return nil
}

func (cc *ConfluentConsumer) logMessageFailure(message string, record *ckafka.Message, err error) {
	payload := mapConfluentToPayload(record)
	cc.logger.Error(message,
		"topic", payload.Topic,
		"partition", payload.Partition,
		"offset", payload.Offset,
		"error", redact.ErrorMessage(err),
	)
}

func (cc *ConfluentConsumer) logBatchFailure(message string, batch BatchPayload, err error) {
	cc.logger.Error(message,
		"topic", batch.Topic,
		"partition", batch.Partition,
		"firstOffset", batch.FirstOffset,
		"lastOffset", batch.LastOffset,
		"error", redact.ErrorMessage(err),
	)
}

func runConfluentPartitionGroups(
	ctx context.Context,
	groups []confluentBatchGroup,
	concurrency int,
	handler func(context.Context, confluentBatchGroup) error,
) error {
	if len(groups) == 0 {
		return nil
	}
	if concurrency < 1 {
		concurrency = 1
	}
	waveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for _, group := range groups {
		select {
		case sem <- struct{}{}:
		case <-waveCtx.Done():
			break
		}
		if waveCtx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(current confluentBatchGroup) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := handler(waveCtx, current); err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
			}
		}(group)
	}
	wg.Wait()
	return firstErr
}

func isConfluentPollTimeout(err error) bool {
	kafkaErr, ok := err.(ckafka.Error)
	return ok && kafkaErr.Code() == ckafka.ErrTimedOut
}

func (cc *ConfluentConsumer) beginConsumeRun() (
	confluentConsumerDriver,
	context.Context,
	func(),
	error,
) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.closed {
		return nil, nil, nil, fmt.Errorf("kafka/confluent: consumer is closed")
	}
	if cc.consumer == nil {
		return nil, nil, nil, fmt.Errorf("kafka/confluent: consumer not connected")
	}
	if cc.runDone != nil {
		return nil, nil, nil, fmt.Errorf("kafka/confluent: consumer is already running")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	cc.cancelFn = cancel
	cc.runDone = done
	finish := func() {
		cancel()
		cc.mu.Lock()
		if cc.runDone == done {
			cc.cancelFn = nil
			cc.runDone = nil
			close(done)
		}
		cc.mu.Unlock()
	}
	return cc.consumer, ctx, finish, nil
}

type confluentBatchGroup struct {
	payload     BatchPayload
	messages    []*ckafka.Message
	lastMessage *ckafka.Message
}

func groupConfluentBatchMessages(messages []*ckafka.Message) []confluentBatchGroup {
	groups := make([]confluentBatchGroup, 0)
	indexes := make(map[string]int)
	for _, message := range messages {
		if message == nil {
			continue
		}
		payload := mapConfluentToPayload(message)
		key := fmt.Sprintf("%s\x00%d", payload.Topic, payload.Partition)
		index, exists := indexes[key]
		if !exists {
			index = len(groups)
			indexes[key] = index
			groups = append(groups, confluentBatchGroup{payload: BatchPayload{
				Topic:       payload.Topic,
				Partition:   payload.Partition,
				FirstOffset: payload.Offset,
			}})
		}
		group := &groups[index]
		group.payload.Messages = append(group.payload.Messages, payload)
		group.messages = append(group.messages, message)
		group.payload.LastOffset = payload.Offset
		group.lastMessage = message
	}
	return groups
}

// Close cancels the active run context, waits for every handler and offset
// operation to finish, and only then closes the driver. This ordering mirrors
// KafkaJS runner.stop and prevents ReadMessage/CommitMessage from racing Close.
func (cc *ConfluentConsumer) Close() error {
	cc.mu.Lock()
	if cc.closeDone != nil {
		done := cc.closeDone
		cc.mu.Unlock()
		<-done
		cc.mu.Lock()
		err := cc.closeErr
		cc.mu.Unlock()
		return err
	}
	cc.closed = true
	cc.closeDone = make(chan struct{})
	done := cc.closeDone
	cancel := cc.cancelFn
	runDone := cc.runDone
	consumer := cc.consumer
	cc.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if runDone != nil {
		<-runDone
	}

	var closeErr error
	if consumer != nil {
		if err := consumer.Close(); err != nil {
			closeErr = fmt.Errorf("kafka/confluent: consumer close failed: %w", err)
		}
	}

	cc.mu.Lock()
	cc.consumer = nil
	cc.closeErr = closeErr
	close(done)
	cc.mu.Unlock()

	if closeErr != nil {
		cc.logger.Error("kafka/confluent: consumer close failed",
			"groupId", cc.groupID,
			"error", redact.ErrorMessage(closeErr),
		)
		return closeErr
	}
	cc.logger.Info("kafka/confluent: consumer closed",
		"groupId", cc.groupID,
	)
	return nil
}

// ---------------------------------------------------------------------------
// InitDefault convenience function
// ---------------------------------------------------------------------------

// InitDefault creates a Client with a real confluent-kafka-go driver, fully
// connected. Go equivalent It resolves
// configuration, creates the client, and wires up the confluent driver so that
// Producer() and Consumer() calls on the returned Client use real Kafka
// connections.
//
// Usage:
//
//	client, err := kafka.InitDefault(nil) // resolve config from env
//	if err != nil { ... }
//	defer client.Driver.Close()
func InitDefault(cfg *Config) (*Client, error) {
	client, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}

	driver, err := NewConfluentClient(client.Config)
	if err != nil {
		return nil, fmt.Errorf("kafka: confluent driver init failed: %w", err)
	}

	client.Driver = driver
	client.Logger.Info("kafka: initialized with confluent-kafka-go driver")
	return client, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// buildConfluentConfig translates a fit Config into a ckafka.ConfigMap.
func buildConfluentConfig(cfg *Config) (*ckafka.ConfigMap, error) {
	cm := &ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),
	}

	// Client identity.
	if cfg.ClientID != "" {
		_ = cm.SetKey("client.id", cfg.ClientID)
	}

	// Producer defaults: acks=all.
	_ = cm.SetKey("acks", "all")

	// Compression: LZ4 by default.
	_ = cm.SetKey("compression.type", mapCompressionToString(cfg.Compression))

	// SASL configuration.
	if cfg.SASL != nil {
		_ = cm.SetKey("security.protocol", securityProtocol(cfg))
		_ = cm.SetKey("sasl.username", cfg.SASL.Username)
		_ = cm.SetKey("sasl.password", cfg.SASL.Password)
		_ = cm.SetKey("sasl.mechanism", mapSASLMechanismToString(cfg.SASL.Mechanism))
	} else if cfg.TLS != nil {
		_ = cm.SetKey("security.protocol", "SSL")
	}

	// TLS configuration.
	if cfg.TLS != nil {
		if cfg.TLS.CAFile != "" {
			_ = cm.SetKey("ssl.ca.location", cfg.TLS.CAFile)
		}
		if cfg.TLS.CertFile != "" {
			_ = cm.SetKey("ssl.certificate.location", cfg.TLS.CertFile)
		}
		if cfg.TLS.KeyFile != "" {
			_ = cm.SetKey("ssl.key.location", cfg.TLS.KeyFile)
		}
		// Match rejectUnauthorized: false
		_ = cm.SetKey("enable.ssl.certificate.verification", false)
	}

	return cm, nil
}

// securityProtocol determines the security.protocol value based on SASL and
// TLS config presence.
func securityProtocol(cfg *Config) string {
	if cfg.SASL != nil && cfg.TLS != nil {
		return "SASL_SSL"
	}
	if cfg.SASL != nil {
		return "SASL_PLAINTEXT"
	}
	if cfg.TLS != nil {
		return "SSL"
	}
	return "PLAINTEXT"
}

// mapCompressionToString converts a fit CompressionType to a librdkafka
// compression.type string.
func mapCompressionToString(ct CompressionType) string {
	switch ct {
	case CompressionGZIP:
		return "gzip"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	case CompressionZSTD:
		return "zstd"
	default:
		return "none"
	}
}

func confluentPartitioner(partitioner ProducerPartitioner) (string, error) {
	switch partitioner {
	case ProducerPartitionerDefault:
		return "", nil
	case ProducerPartitionerKafkaJSCompatible:
		return "murmur2_random", nil
	default:
		return "", fmt.Errorf("kafka/confluent: unsupported producer partitioner %q", partitioner)
	}
}

const (
	defaultProducerMetadataTimeout = 30 * time.Second
	// KafkaJS keeps cluster metadata for five minutes by default. Reusing the
	// same lifetime avoids a synchronous metadata request for every keyless
	// produce while retaining its observable partition-selection behavior.
	defaultProducerMetadataMaxAge = 5 * time.Minute
)

func (cp *ConfluentProducer) buildBrokerMessages(
	ctx context.Context,
	producer confluentProducerDriver,
	topic string,
	messages []Message,
) ([]*ckafka.Message, error) {
	brokerMessages := make([]*ckafka.Message, 0, len(messages))
	needsKafkaJSKeylessPartition := false
	for _, msg := range messages {
		if cp.partitioner == ProducerPartitionerKafkaJSCompatible && msg.Partition < 0 && msg.Key == nil {
			needsKafkaJSKeylessPartition = true
			break
		}
	}

	var partitionMetadata []ckafka.PartitionMetadata
	if needsKafkaJSKeylessPartition {
		metadataDriver, ok := producer.(confluentProducerMetadataDriver)
		if !ok {
			return nil, fmt.Errorf("kafka/confluent: KafkaJS legacy keyless partitioning requires producer metadata")
		}
		var err error
		partitionMetadata, err = cp.kafkaJSPartitionMetadata(ctx, metadataDriver, topic)
		if err != nil {
			return nil, err
		}
	}

	for _, msg := range messages {
		brokerMessage := buildConfluentMessage(topic, msg)
		if cp.partitioner == ProducerPartitionerKafkaJSCompatible && msg.Partition < 0 && msg.Key == nil {
			partition, err := cp.nextKafkaJSKeylessPartition(topic, partitionMetadata)
			if err != nil {
				return nil, err
			}
			brokerMessage.TopicPartition.Partition = partition
		}
		brokerMessages = append(brokerMessages, brokerMessage)
	}
	return brokerMessages, nil
}

func (cp *ConfluentProducer) kafkaJSPartitionMetadata(
	ctx context.Context,
	driver confluentProducerMetadataDriver,
	topic string,
) ([]ckafka.PartitionMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	now := time.Now
	if cp.metadataNow != nil {
		now = cp.metadataNow
	}
	currentTime := now()

	cp.metadataMu.Lock()
	if cached, ok := cp.metadataCache[topic]; ok && currentTime.Before(cached.expiresAt) {
		partitions := clonePartitionMetadata(cached.partitions)
		cp.metadataMu.Unlock()
		return partitions, nil
	}
	if refresh := cp.metadataRefreshes[topic]; refresh != nil {
		cp.metadataMu.Unlock()
		select {
		case <-refresh.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return clonePartitionMetadata(refresh.partitions), refresh.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if cp.metadataRefreshes == nil {
		cp.metadataRefreshes = make(map[string]*kafkaJSMetadataRefresh)
	}
	refresh := &kafkaJSMetadataRefresh{done: make(chan struct{})}
	cp.metadataRefreshes[topic] = refresh
	cp.metadataMu.Unlock()

	partitions, err := cp.fetchKafkaJSPartitionMetadata(driver, topic)

	cp.metadataMu.Lock()
	refresh.partitions = clonePartitionMetadata(partitions)
	refresh.err = err
	if err == nil {
		maxAge := cp.metadataMaxAge
		if maxAge <= 0 {
			maxAge = defaultProducerMetadataMaxAge
		}
		if cp.metadataCache == nil {
			cp.metadataCache = make(map[string]kafkaJSMetadataCacheEntry)
		}
		cp.metadataCache[topic] = kafkaJSMetadataCacheEntry{
			partitions: clonePartitionMetadata(partitions),
			expiresAt:  now().Add(maxAge),
		}
	}
	delete(cp.metadataRefreshes, topic)
	close(refresh.done)
	cp.metadataMu.Unlock()

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	return partitions, err
}

func (cp *ConfluentProducer) fetchKafkaJSPartitionMetadata(
	driver confluentProducerMetadataDriver,
	topic string,
) ([]ckafka.PartitionMetadata, error) {
	timeout := cp.metadataTimeout
	if timeout <= 0 {
		timeout = defaultProducerMetadataTimeout
	}
	metadata, err := driver.GetMetadata(&topic, false, int(timeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("kafka/confluent: metadata for topic %s: %w", topic, err)
	}
	if metadata == nil {
		return nil, fmt.Errorf("kafka/confluent: metadata for topic %s was nil", topic)
	}
	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("kafka/confluent: metadata for topic %s was not returned", topic)
	}
	if topicMetadata.Error.Code() != ckafka.ErrNoError {
		return nil, fmt.Errorf("kafka/confluent: metadata for topic %s: %w", topic, topicMetadata.Error)
	}
	if len(topicMetadata.Partitions) == 0 {
		return nil, fmt.Errorf("kafka/confluent: topic %s has no partitions", topic)
	}
	return clonePartitionMetadata(topicMetadata.Partitions), nil
}

func clonePartitionMetadata(partitions []ckafka.PartitionMetadata) []ckafka.PartitionMetadata {
	if partitions == nil {
		return nil
	}
	return append([]ckafka.PartitionMetadata(nil), partitions...)
}

func (cp *ConfluentProducer) invalidateKafkaJSMetadata(topics ...string) {
	if cp.partitioner != ProducerPartitionerKafkaJSCompatible || len(topics) == 0 {
		return
	}
	cp.metadataMu.Lock()
	for _, topic := range topics {
		delete(cp.metadataCache, topic)
	}
	cp.metadataMu.Unlock()
}

func (cp *ConfluentProducer) invalidateKafkaJSMetadataOnError(err error, topics ...string) {
	if !isKafkaTopologyError(err) {
		return
	}
	cp.invalidateKafkaJSMetadata(topics...)
}

func isKafkaTopologyError(err error) bool {
	if err == nil {
		return false
	}
	var kafkaErr ckafka.Error
	if !errors.As(err, &kafkaErr) {
		return false
	}
	switch kafkaErr.Code() {
	case ckafka.ErrUnknownPartition,
		ckafka.ErrUnknownTopic,
		ckafka.ErrUnknownTopicOrPart,
		ckafka.ErrUnknownTopicID,
		ckafka.ErrLeaderNotAvailable,
		ckafka.ErrNotLeaderForPartition,
		ckafka.ErrBrokerNotAvailable,
		ckafka.ErrReplicaNotAvailable,
		ckafka.ErrInvalidPartitions:
		return true
	default:
		return false
	}
}

func (cp *ConfluentProducer) nextKafkaJSKeylessPartition(
	topic string,
	partitionMetadata []ckafka.PartitionMetadata,
) (int32, error) {
	available := make([]int32, 0, len(partitionMetadata))
	for _, partition := range partitionMetadata {
		if partition.Leader >= 0 {
			available = append(available, partition.ID)
		}
	}

	cp.partitionMu.Lock()
	defer cp.partitionMu.Unlock()
	if cp.partitionCounters == nil {
		cp.partitionCounters = make(map[string]uint32)
	}
	counter, ok := cp.partitionCounters[topic]
	if !ok {
		seed := cp.partitionSeed
		if seed == nil {
			seed = kafkaJSPartitionCounterSeed
		}
		var err error
		counter, err = seed()
		if err != nil {
			return 0, fmt.Errorf("kafka/confluent: seed KafkaJS partition counter: %w", err)
		}
	}
	counter++
	cp.partitionCounters[topic] = counter
	positive := counter & 0x7fffffff
	if len(available) > 0 {
		return available[int(positive%uint32(len(available)))], nil
	}
	if len(partitionMetadata) == 0 {
		return 0, fmt.Errorf("kafka/confluent: topic %s has no partitions", topic)
	}
	// KafkaJS returns the array index in this no-leader fallback, rather than
	// the partitionId field.
	return int32(positive % uint32(len(partitionMetadata))), nil
}

func kafkaJSPartitionCounterSeed() (uint32, error) {
	var randomBytes [32]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(randomBytes[:4]), nil
}

// mapSASLMechanismToString converts a string SASL mechanism name to the
// librdkafka sasl.mechanism value.
func mapSASLMechanismToString(mechanism string) string {
	switch strings.ToUpper(mechanism) {
	case "SCRAM-SHA-256":
		return "SCRAM-SHA-256"
	case "SCRAM-SHA-512":
		return "SCRAM-SHA-512"
	default:
		return "PLAIN"
	}
}

// buildConfluentMessage converts a fit Message to a ckafka.Message.
func buildConfluentMessage(topic string, msg Message) *ckafka.Message {
	km := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Value: msg.Value,
	}

	if msg.Key != nil {
		km.Key = msg.Key
	}

	if msg.Partition >= 0 {
		km.TopicPartition.Partition = int32(msg.Partition)
	}

	if !msg.Timestamp.IsZero() {
		km.Timestamp = msg.Timestamp
	}

	// Map headers.
	if len(msg.Headers) > 0 {
		headers := make([]ckafka.Header, len(msg.Headers))
		for i, h := range msg.Headers {
			headers[i] = ckafka.Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
		km.Headers = headers
	}

	return km
}

// mapConfluentToPayload converts a ckafka.Message to a fit MessagePayload.
func mapConfluentToPayload(msg *ckafka.Message) MessagePayload {
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	payload := MessagePayload{
		Topic:     topic,
		Partition: int(msg.TopicPartition.Partition),
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: msg.Timestamp,
	}

	if len(msg.Headers) > 0 {
		headers := make([]Header, len(msg.Headers))
		for i, h := range msg.Headers {
			headers[i] = Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
		payload.Headers = headers
	}

	return payload
}

// mapConfluentToRecordMetadata converts a delivery report into the same
// metadata shape legacy fit.js exposes for router produce responses.
func mapConfluentToRecordMetadata(msg *ckafka.Message) RecordMetadata {
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	return RecordMetadata{
		Topic:      topic,
		Offset:     int64(msg.TopicPartition.Offset),
		TopicName:  topic,
		Partition:  int(msg.TopicPartition.Partition),
		ErrorCode:  0,
		BaseOffset: fmt.Sprintf("%d", msg.TopicPartition.Offset),
	}
}

// cloneConfigMap creates a copy of a ckafka.ConfigMap. Since ConfigMap is a
// map[string]ConfigValue, we iterate and copy each key.
func cloneConfigMap(src *ckafka.ConfigMap) *ckafka.ConfigMap {
	clone := ckafka.ConfigMap{}
	for k, v := range *src {
		clone[k] = v
	}
	return &clone
}

// resolveAutoCommit determines the effective auto-commit setting. The
// per-run override (from ConsumerOptions) takes precedence over the
// consumer config default.
func resolveAutoCommit(configDefault bool, override *bool) bool {
	if override != nil {
		return *override
	}
	return configDefault
}

func validateConfluentConsumerOptions(
	configAutoCommit bool,
	opts ConsumerOptions,
	defaultPollTimeout time.Duration,
) (bool, time.Duration, int, error) {
	if opts.PartitionsConsumedConcurrently < 0 {
		return false, 0, 0, fmt.Errorf("kafka/confluent: PartitionsConsumedConcurrently must not be negative")
	}
	if opts.PollTimeout < 0 {
		return false, 0, 0, fmt.Errorf("kafka/confluent: PollTimeout must not be negative")
	}
	if opts.MaxRecords < 0 {
		return false, 0, 0, fmt.Errorf("kafka/confluent: MaxRecords must not be negative")
	}
	effectiveAutoCommit := resolveAutoCommit(configAutoCommit, opts.AutoCommit)
	if opts.NullOffsetCommitMetadata {
		if effectiveAutoCommit {
			return false, 0, 0, fmt.Errorf("kafka/confluent: NullOffsetCommitMetadata requires manual commit")
		}
		if opts.CommitBeforeHandler {
			return false, 0, 0, fmt.Errorf("kafka/confluent: NullOffsetCommitMetadata cannot be combined with CommitBeforeHandler")
		}
	}
	if opts.OffsetFinalizer != nil {
		if effectiveAutoCommit {
			return false, 0, 0, fmt.Errorf("kafka/confluent: OffsetFinalizer requires manual commit")
		}
		if opts.CommitBeforeHandler {
			return false, 0, 0, fmt.Errorf("kafka/confluent: OffsetFinalizer cannot be combined with CommitBeforeHandler")
		}
	} else {
		if opts.ResolveAfterSuccessfulFinalizer {
			return false, 0, 0, fmt.Errorf("kafka/confluent: ResolveAfterSuccessfulFinalizer requires OffsetFinalizer")
		}
	}

	pollTimeout := opts.PollTimeout
	if pollTimeout == 0 {
		pollTimeout = defaultPollTimeout
	}
	concurrency := opts.PartitionsConsumedConcurrently
	if concurrency == 0 {
		concurrency = 1
	}
	return effectiveAutoCommit, pollTimeout, concurrency, nil
}
