package kafka

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gofynd/fit-go/logging"
)

// TestKafkaJSCompatibleUnresolvedFinalizerMultiRecordLive proves the exact
// compatibility behavior against a disposable broker. Two same-partition
// records are present before the first poll (and MaxRecords is two), so the
// failed N and following N+1 are returned in one PollRecords batch. The opt-in
// must stop that batch, replay N, then deliver N+1. The default-false case pins
// the existing behavior for every consumer that does not request compatibility.
func TestKafkaJSCompatibleUnresolvedFinalizerMultiRecordLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	cases := []struct {
		name      string
		redeliver bool
		want      []int64
	}{
		{name: "opt_in_replays_failed_N_before_N_plus_1", redeliver: true, want: []int64{0, 0, 1}},
		{name: "default_false_preserves_existing_fetch_position", want: []int64{0, 1}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			runKafkaJSUnresolvedMultiRecordFixture(t, broker, test.redeliver, test.want)
		})
	}
}

// TestKafkaJSCompatibleCloseDrainsInFlightHandlerLive proves the shutdown
// ordering required by BlockRebalanceOnPoll. Close must cancel the handler but
// retain the assignment until that handler reaches its offset boundary; only
// then may it allow the final rebalance and close the client.
func TestKafkaJSCompatibleCloseDrainsInFlightHandlerLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "fit-go-close-drain-" + suffix
	group := "fit-go-close-drain-group-" + suffix

	admin, err := confluentKafka.NewAdminClient(&confluentKafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatal(err)
	}
	results, err := admin.CreateTopics(ctx, []confluentKafka.TopicSpecification{{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}})
	admin.Close()
	if err != nil || len(results) != 1 || (results[0].Error.Code() != confluentKafka.ErrNoError && results[0].Error.Code() != confluentKafka.ErrTopicAlreadyExists) {
		t.Fatalf("create topic: results=%#v err=%v", results, err)
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("drain-me")}).FirstErr(); err != nil {
		producer.Close()
		t.Fatalf("produce fixture: %v", err)
	}
	producer.Close()

	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	firstRevoked := make(chan []PartitionAssignment, 4)
	consumerConfig := DefaultConsumerConfig(group)
	consumerConfig.Backend = ConsumerBackendKafkaJSCompatible
	consumerConfig.AutoCommit = false
	consumerConfig.OnPartitionsRevoked = func(revoked []PartitionAssignment) {
		firstRevoked <- append([]PartitionAssignment(nil), revoked...)
	}
	consumer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "fit-go-close-drain-test"}, consumerConfig, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	releaseHandler := make(chan struct{})
	var releaseHandlerOnce sync.Once
	release := func() { releaseHandlerOnce.Do(func() { close(releaseHandler) }) }
	t.Cleanup(release)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.ConsumeCtx(func(handlerCtx context.Context, payload MessagePayload) error {
			if string(payload.Value) != "drain-me" {
				return fmt.Errorf("unexpected payload %q", payload.Value)
			}
			close(handlerStarted)
			<-handlerCtx.Done()
			close(handlerCanceled)
			<-releaseHandler
			return handlerCtx.Err()
		}, ConsumerOptions{PollTimeout: 100 * time.Millisecond, MaxRecords: 1})
	}()

	select {
	case <-handlerStarted:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the handler to start")
	}

	// A second member starts the rebalance while the first member's poll gate is
	// held by the blocked handler. The peer must not receive the partition, and
	// the first member must not revoke it, until that handler reaches its offset
	// boundary.
	peerAssigned := make(chan []PartitionAssignment, 4)
	peerConfig := DefaultConsumerConfig(group)
	peerConfig.Backend = ConsumerBackendKafkaJSCompatible
	peerConfig.AutoCommit = false
	peerConfig.OnPartitionsAssigned = func(assigned []PartitionAssignment) {
		peerAssigned <- append([]PartitionAssignment(nil), assigned...)
	}
	peer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "fit-go-close-drain-peer"}, peerConfig, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	peerConsumeDone := make(chan error, 1)
	go func() {
		peerConsumeDone <- peer.ConsumeCtx(func(context.Context, MessagePayload) error { return nil }, ConsumerOptions{
			PollTimeout: 100 * time.Millisecond,
			MaxRecords:  1,
		})
	}()
	waitForKafkaJSRuntimeGroupMembers(t, ctx, broker, group, 2)
	assertNoKafkaJSRuntimeRebalance(t, firstRevoked, peerAssigned, 300*time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- consumer.Close() }()
	select {
	case <-handlerCanceled:
	case <-ctx.Done():
		t.Fatal("Close did not cancel the in-flight handler")
	}
	assertNoKafkaJSRuntimeRebalance(t, firstRevoked, peerAssigned, 300*time.Millisecond)
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the in-flight handler drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()

	firstFinalRevocation := waitForKafkaJSRuntimePartitions(t, ctx, firstRevoked, topic, 1)
	if firstFinalRevocation[0].Partition != 0 {
		t.Fatalf("first consumer revoked partition %d, want 0", firstFinalRevocation[0].Partition)
	}
	peerFinalAssignment := waitForKafkaJSRuntimePartitions(t, ctx, peerAssigned, topic, 1)
	if peerFinalAssignment[0].Partition != 0 {
		t.Fatalf("peer received partition %d, want 0", peerFinalAssignment[0].Partition)
	}

	select {
	case err := <-consumeDone:
		if err != nil {
			t.Fatalf("Consume returned a shutdown error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("consumer run did not finish after handler release")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not finish after handler release")
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}
	select {
	case err := <-peerConsumeDone:
		if err != nil {
			t.Fatalf("peer consumer shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("peer consumer did not stop")
	}
}

// TestKafkaJSCompatibleNullOffsetCommitMetadataLive exercises the supported
// franz-go pre-commit hook used for KafkaJS's null offset metadata. It proves
// that an exact N+1 finalizer commit reaches the coordinator with nil metadata,
// rather than relying only on a request-construction unit test.
func TestKafkaJSCompatibleNullOffsetCommitMetadataLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "fit-go-null-offset-metadata-" + suffix
	group := "fit-go-null-offset-metadata-group-" + suffix
	createKafkaJSRuntimeTopic(t, ctx, broker, topic, 1)

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("commit-me")}).FirstErr(); err != nil {
		producer.Close()
		t.Fatalf("produce fixture: %v", err)
	}
	producer.Close()

	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	consumerConfig := DefaultConsumerConfig(group)
	consumerConfig.Backend = ConsumerBackendKafkaJSCompatible
	consumerConfig.AutoCommit = false
	consumer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "fit-go-null-offset-metadata-test"}, consumerConfig, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	finalized := make(chan int64, 1)
	consumeDone := make(chan error, 1)
	manualCommit := false
	go func() {
		consumeDone <- consumer.ConsumeCtx(func(_ context.Context, payload MessagePayload) error {
			if string(payload.Value) != "commit-me" {
				return fmt.Errorf("unexpected payload %q", payload.Value)
			}
			return nil
		}, ConsumerOptions{
			AutoCommit: &manualCommit,
			OffsetFinalizer: func(_ context.Context, payload MessagePayload, handlerErr error, commit ExactOffsetCommit) error {
				if handlerErr != nil {
					return handlerErr
				}
				exact := payload.Offset + 1
				if err := commit(exact); err != nil {
					return err
				}
				finalized <- exact
				return nil
			},
			NullOffsetCommitMetadata: true,
			PollTimeout:              100 * time.Millisecond,
			MaxRecords:               1,
		})
	}()

	var wantOffset int64
	select {
	case wantOffset = <-finalized:
	case err := <-consumeDone:
		t.Fatalf("consumer ended before exact commit: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for exact commit")
	}
	committed := waitForKafkaJSCommittedTopicPartition(t, ctx, broker, group, topic, wantOffset)
	if committed.Metadata != nil {
		t.Fatalf("committed metadata = %q, want nil", *committed.Metadata)
	}

	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-consumeDone:
		if err != nil {
			t.Fatalf("consumer shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not stop")
	}
}

// TestKafkaJSCompatibleOrdinaryManualCommitNullMetadataLive covers the normal
// handler-success path used by Mixmaster: auto commit is disabled, no custom
// finalizer is installed, and ConsumeCtx commits offset+1 after a nil handler
// result. The coordinator must persist KafkaJS-compatible null metadata.
func TestKafkaJSCompatibleOrdinaryManualCommitNullMetadataLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "fit-go-ordinary-null-offset-metadata-" + suffix
	group := "fit-go-ordinary-null-offset-metadata-group-" + suffix
	createKafkaJSRuntimeTopic(t, ctx, broker, topic, 1)

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("ordinary-commit")}).FirstErr(); err != nil {
		producer.Close()
		t.Fatalf("produce fixture: %v", err)
	}
	producer.Close()

	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	consumerConfig := DefaultConsumerConfig(group)
	consumerConfig.Backend = ConsumerBackendKafkaJSCompatible
	consumerConfig.AutoCommit = false
	consumer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "fit-go-ordinary-null-offset-metadata-test"}, consumerConfig, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	manualCommit := false
	handled := make(chan int64, 1)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.ConsumeCtx(func(_ context.Context, payload MessagePayload) error {
			if string(payload.Value) != "ordinary-commit" {
				return fmt.Errorf("unexpected payload %q", payload.Value)
			}
			handled <- payload.Offset + 1
			return nil
		}, ConsumerOptions{
			AutoCommit:               &manualCommit,
			NullOffsetCommitMetadata: true,
			PollTimeout:              100 * time.Millisecond,
			MaxRecords:               1,
		})
	}()

	var wantOffset int64
	select {
	case wantOffset = <-handled:
	case err := <-consumeDone:
		t.Fatalf("consumer ended before handling the record: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for ordinary handler")
	}
	committed := waitForKafkaJSCommittedTopicPartition(t, ctx, broker, group, topic, wantOffset)
	if committed.Metadata != nil {
		t.Fatalf("committed metadata = %q, want nil", *committed.Metadata)
	}

	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-consumeDone:
		if err != nil {
			t.Fatalf("consumer shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not stop")
	}
}

// TestKafkaJSCompatibleReadCommittedLive proves that the opt-in fetch isolation
// hides an aborted transactional record while still delivering the following
// committed record on the same partition.
func TestKafkaJSCompatibleReadCommittedLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "fit-go-read-committed-" + suffix
	group := "fit-go-read-committed-group-" + suffix
	createKafkaJSRuntimeTopic(t, ctx, broker, topic, 1)

	transactionalProducer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.TransactionalID("fit-go-read-committed-txn-"+suffix),
		kgo.TransactionTimeout(30*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := transactionalProducer.BeginTransaction(); err != nil {
		transactionalProducer.Close()
		t.Fatalf("begin transaction: %v", err)
	}
	if err := transactionalProducer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("aborted")}).FirstErr(); err != nil {
		transactionalProducer.Close()
		t.Fatalf("produce aborted fixture: %v", err)
	}
	if err := transactionalProducer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		transactionalProducer.Close()
		t.Fatalf("abort transaction: %v", err)
	}
	transactionalProducer.Close()

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("committed")}).FirstErr(); err != nil {
		producer.Close()
		t.Fatalf("produce committed fixture: %v", err)
	}
	producer.Close()

	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	consumerConfig := DefaultConsumerConfig(group)
	consumerConfig.Backend = ConsumerBackendKafkaJSCompatible
	consumerConfig.AutoCommit = false
	consumerConfig.ReadCommitted = true
	consumer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "fit-go-read-committed-test"}, consumerConfig, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	manualCommit := false
	received := make(chan string, 4)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.ConsumeCtx(func(_ context.Context, payload MessagePayload) error {
			received <- string(payload.Value)
			return nil
		}, ConsumerOptions{
			AutoCommit:  &manualCommit,
			PollTimeout: 100 * time.Millisecond,
			MaxRecords:  1,
		})
	}()

	select {
	case value := <-received:
		if value != "committed" {
			t.Fatalf("first visible record = %q, want committed", value)
		}
	case err := <-consumeDone:
		t.Fatalf("consumer ended before committed record: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for committed record")
	}
	select {
	case value := <-received:
		t.Fatalf("unexpected additional visible record %q", value)
	case <-time.After(500 * time.Millisecond):
	}

	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-consumeDone:
		if err != nil {
			t.Fatalf("consumer shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not stop")
	}
}

// TestKafkaJSCompatibleMixedLegacyGroupLive runs the actual legacy KafkaJS
// round-robin assigner and the franz-go compatibility backend in one classic
// consumer group. It proves join, split assignment, rebalance, and leave using
// the literal protocol that must remain compatible during a Node-to-Go rollout.
func TestKafkaJSCompatibleMixedLegacyGroupLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	kafkaJSModule := strings.TrimSpace(os.Getenv("FIT_GO_KAFKAJS_NODE_MODULE"))
	expectedVersion := strings.TrimSpace(os.Getenv("FIT_GO_KAFKAJS_EXPECTED_VERSION"))
	if broker == "" || kafkaJSModule == "" || expectedVersion == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER, FIT_GO_KAFKAJS_NODE_MODULE, and FIT_GO_KAFKAJS_EXPECTED_VERSION for the mixed-client test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Run("node_member_starts_first", func(t *testing.T) {
		runKafkaJSMixedLegacyGroupFixture(t, ctx, broker, kafkaJSModule, expectedVersion, false)
	})
	t.Run("go_member_starts_first", func(t *testing.T) {
		runKafkaJSMixedLegacyGroupFixture(t, ctx, broker, kafkaJSModule, expectedVersion, true)
	})
}

func runKafkaJSMixedLegacyGroupFixture(
	t *testing.T,
	ctx context.Context,
	broker string,
	kafkaJSModule string,
	expectedVersion string,
	goMemberStartsFirst bool,
) {
	t.Helper()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "fit-go-mixed-kafkajs-" + suffix
	group := "fit-go-mixed-kafkajs-group-" + suffix
	createKafkaJSRuntimeTopic(t, ctx, broker, topic, 4)

	goAssignments := make(chan []PartitionAssignment, 8)
	goRevocations := make(chan []PartitionAssignment, 8)
	startGo := func() (KafkaConsumer, <-chan error) {
		logger, err := logging.New(logging.Options{Level: "error"})
		if err != nil {
			t.Fatal(err)
		}
		consumerConfig := DefaultConsumerConfig(group)
		consumerConfig.Backend = ConsumerBackendKafkaJSCompatible
		consumerConfig.AutoCommit = false
		consumerConfig.OnPartitionsAssigned = func(assigned []PartitionAssignment) {
			goAssignments <- append([]PartitionAssignment(nil), assigned...)
		}
		consumerConfig.OnPartitionsRevoked = func(revoked []PartitionAssignment) {
			goRevocations <- append([]PartitionAssignment(nil), revoked...)
		}
		consumer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "franz-go-live-test"}, consumerConfig, logger)
		if err != nil {
			t.Fatal(err)
		}
		if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = consumer.Close() })
		consumeDone := make(chan error, 1)
		go func() {
			consumeDone <- consumer.ConsumeCtx(func(context.Context, MessagePayload) error { return nil }, ConsumerOptions{
				PollTimeout: 100 * time.Millisecond,
				MaxRecords:  1,
			})
		}()
		return consumer, consumeDone
	}

	if !goMemberStartsFirst {
		node := startLegacyKafkaJSRuntimeProcess(t, ctx, broker, group, topic, kafkaJSModule)
		node.waitForVersion(t, ctx, expectedVersion)
		_ = node.waitForPartitions(t, ctx, topic, 4)

		consumer, consumeDone := startGo()
		goSplit := waitForKafkaJSRuntimePartitions(t, ctx, goAssignments, topic, 2)
		nodeSplit := node.waitForPartitions(t, ctx, topic, 2)
		assertKafkaJSMixedPartitionCoverage(t, topic, nodeSplit, goSplit)

		closeKafkaJSRuntimeConsumer(t, ctx, consumer, consumeDone)
		_ = node.waitForPartitions(t, ctx, topic, 4)
		stopLegacyKafkaJSRuntimeProcess(t, node)
		return
	}

	consumer, consumeDone := startGo()
	_ = waitForKafkaJSRuntimePartitions(t, ctx, goAssignments, topic, 4)

	node := startLegacyKafkaJSRuntimeProcess(t, ctx, broker, group, topic, kafkaJSModule)
	node.waitForVersion(t, ctx, expectedVersion)
	// The pre-existing Go member must acknowledge revocation of its original
	// four-partition assignment before the group settles into a 2+2 split.
	_ = waitForKafkaJSRuntimePartitions(t, ctx, goRevocations, topic, 4)
	goSplit := waitForKafkaJSRuntimePartitions(t, ctx, goAssignments, topic, 2)
	nodeSplit := node.waitForPartitions(t, ctx, topic, 2)
	assertKafkaJSMixedPartitionCoverage(t, topic, nodeSplit, goSplit)

	stopLegacyKafkaJSRuntimeProcess(t, node)
	_ = waitForKafkaJSRuntimePartitions(t, ctx, goAssignments, topic, 4)
	closeKafkaJSRuntimeConsumer(t, ctx, consumer, consumeDone)
}

const legacyKafkaJSRuntimeScript = `
const path = require('path');
const modulePath = process.env.KAFKAJS_MODULE;
const { Kafka, logLevel, PartitionAssigners } = require(modulePath);
const { version } = require(path.join(modulePath, 'package.json'));
process.stdout.write(JSON.stringify({ type: 'version', version }) + '\n');
const consumer = new Kafka({
  clientId: 'legacy-kafkajs-live-test',
  brokers: [process.env.BROKER],
  logLevel: logLevel.NOTHING,
}).consumer({
  groupId: process.env.GROUP,
  partitionAssigners: [PartitionAssigners.roundRobin],
});
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await consumer.disconnect();
  process.exit(0);
}
process.on('SIGTERM', () => { stop().catch(error => { console.error(error); process.exit(1); }); });
consumer.on(consumer.events.GROUP_JOIN, event => {
  process.stdout.write(JSON.stringify({ type: 'assignment', assignment: event.payload.memberAssignment }) + '\n');
});
(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: process.env.TOPIC, fromBeginning: true });
  await consumer.run({ eachMessage: async () => {} });
})().catch(error => { console.error(error); process.exit(1); });
`

type legacyKafkaJSRuntimeEvent struct {
	Type       string             `json:"type"`
	Version    string             `json:"version"`
	Assignment map[string][]int32 `json:"assignment"`
}

type legacyKafkaJSRuntimeProcess struct {
	command *exec.Cmd
	events  chan legacyKafkaJSRuntimeEvent
	done    chan struct{}
	scanErr chan error
	stderr  bytes.Buffer

	waitMu   sync.Mutex
	waitErr  error
	stopOnce sync.Once
	stopErr  error
}

func startLegacyKafkaJSRuntimeProcess(
	t *testing.T,
	ctx context.Context,
	broker string,
	group string,
	topic string,
	kafkaJSModule string,
) *legacyKafkaJSRuntimeProcess {
	t.Helper()
	command := exec.CommandContext(ctx, "node", "-e", legacyKafkaJSRuntimeScript)
	command.Env = append(os.Environ(),
		"BROKER="+broker,
		"GROUP="+group,
		"TOPIC="+topic,
		"KAFKAJS_MODULE="+kafkaJSModule,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &legacyKafkaJSRuntimeProcess{
		command: command,
		events:  make(chan legacyKafkaJSRuntimeEvent, 32),
		done:    make(chan struct{}),
		scanErr: make(chan error, 1),
	}
	command.Stderr = &process.stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1024), 1024*1024)
		for scanner.Scan() {
			var event legacyKafkaJSRuntimeEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				continue
			}
			select {
			case process.events <- event:
			case <-ctx.Done():
				process.scanErr <- ctx.Err()
				close(process.events)
				return
			}
		}
		process.scanErr <- scanner.Err()
		close(process.events)
	}()
	go func() {
		err := command.Wait()
		process.waitMu.Lock()
		process.waitErr = err
		process.waitMu.Unlock()
		close(process.done)
	}()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = process.stop(cleanupCtx)
	})
	return process
}

func (p *legacyKafkaJSRuntimeProcess) waitForVersion(t *testing.T, ctx context.Context, want string) {
	t.Helper()
	for {
		event := p.nextEvent(t, ctx)
		if event.Type != "version" {
			continue
		}
		if event.Version != want {
			t.Fatalf("legacy KafkaJS version = %q, want explicitly audited version %q", event.Version, want)
		}
		return
	}
}

func (p *legacyKafkaJSRuntimeProcess) waitForPartitions(t *testing.T, ctx context.Context, topic string, want int) map[string][]int32 {
	t.Helper()
	for {
		event := p.nextEvent(t, ctx)
		if event.Type == "assignment" && len(event.Assignment[topic]) == want {
			return event.Assignment
		}
	}
}

func (p *legacyKafkaJSRuntimeProcess) nextEvent(t *testing.T, ctx context.Context) legacyKafkaJSRuntimeEvent {
	t.Helper()
	select {
	case event, ok := <-p.events:
		if ok {
			return event
		}
		var scannerErr error
		select {
		case scannerErr = <-p.scanErr:
		default:
		}
		var processErr error
		var stderr string
		select {
		case <-p.done:
			processErr = p.waitResult()
			stderr = p.stderr.String()
		default:
		}
		t.Fatalf("legacy KafkaJS event stream closed: process=%v scanner=%v stderr=%q", processErr, scannerErr, stderr)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for legacy KafkaJS event: %v", ctx.Err())
	}
	return legacyKafkaJSRuntimeEvent{}
}

func (p *legacyKafkaJSRuntimeProcess) waitResult() error {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

func (p *legacyKafkaJSRuntimeProcess) stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		select {
		case <-p.done:
			p.stopErr = p.waitResult()
			return
		default:
		}
		if err := p.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			p.stopErr = fmt.Errorf("signal legacy KafkaJS process: %w", err)
			_ = p.command.Process.Kill()
			<-p.done
			return
		}
		select {
		case <-p.done:
			p.stopErr = p.waitResult()
		case <-ctx.Done():
			_ = p.command.Process.Kill()
			<-p.done
			p.stopErr = fmt.Errorf("stop legacy KafkaJS process: %w", ctx.Err())
		}
	})
	return p.stopErr
}

func stopLegacyKafkaJSRuntimeProcess(t *testing.T, process *legacyKafkaJSRuntimeProcess) {
	t.Helper()
	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := process.stop(stopCtx); err != nil {
		t.Fatalf("legacy KafkaJS shutdown: %v; stderr=%q", err, process.stderr.String())
	}
}

func closeKafkaJSRuntimeConsumer(t *testing.T, ctx context.Context, consumer KafkaConsumer, consumeDone <-chan error) {
	t.Helper()
	closeDone := make(chan error, 1)
	go func() { closeDone <- consumer.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close franz-go consumer: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("franz-go consumer close timed out")
	}
	select {
	case err := <-consumeDone:
		if err != nil {
			t.Fatalf("franz-go consumer shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("franz-go consumer run did not stop")
	}
}

func assertKafkaJSMixedPartitionCoverage(
	t *testing.T,
	topic string,
	nodeAssignment map[string][]int32,
	goAssignment []PartitionAssignment,
) {
	t.Helper()
	seen := make(map[int32]string, 4)
	for _, partition := range nodeAssignment[topic] {
		if partition < 0 || partition >= 4 {
			t.Fatalf("legacy KafkaJS received invalid partition %d", partition)
		}
		seen[partition] = "node"
	}
	for _, assignment := range goAssignment {
		if assignment.Topic != topic {
			continue
		}
		if assignment.Partition < 0 || assignment.Partition >= 4 {
			t.Fatalf("franz-go received invalid partition %d", assignment.Partition)
		}
		if owner, exists := seen[assignment.Partition]; exists {
			t.Fatalf("partition %d assigned to both %s and franz-go", assignment.Partition, owner)
		}
		seen[assignment.Partition] = "go"
	}
	if len(nodeAssignment[topic]) != 2 || len(goAssignment) != 2 || len(seen) != 4 {
		t.Fatalf("mixed group must have a disjoint 2+2 split across four partitions: node=%v go=%v coverage=%v", nodeAssignment[topic], goAssignment, seen)
	}
}

func createKafkaJSRuntimeTopic(t *testing.T, ctx context.Context, broker string, topic string, partitions int) {
	t.Helper()
	admin, err := confluentKafka.NewAdminClient(&confluentKafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatal(err)
	}
	results, err := admin.CreateTopics(ctx, []confluentKafka.TopicSpecification{{
		Topic: topic, NumPartitions: partitions, ReplicationFactor: 1,
	}})
	admin.Close()
	if err != nil || len(results) != 1 || (results[0].Error.Code() != confluentKafka.ErrNoError && results[0].Error.Code() != confluentKafka.ErrTopicAlreadyExists) {
		t.Fatalf("create topic: results=%#v err=%v", results, err)
	}
}

func waitForKafkaJSRuntimePartitions(
	t *testing.T,
	ctx context.Context,
	events <-chan []PartitionAssignment,
	topic string,
	want int,
) []PartitionAssignment {
	t.Helper()
	for {
		select {
		case assignments := <-events:
			matching := make([]PartitionAssignment, 0, len(assignments))
			for _, assignment := range assignments {
				if assignment.Topic == topic {
					matching = append(matching, assignment)
				}
			}
			if len(matching) == want {
				return matching
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %d partitions on %s: %v", want, topic, ctx.Err())
		}
	}
}

func assertNoKafkaJSRuntimeRebalance(
	t *testing.T,
	revocations <-chan []PartitionAssignment,
	peerAssignments <-chan []PartitionAssignment,
	duration time.Duration,
) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case revoked := <-revocations:
		t.Fatalf("first consumer revoked while its handler was still blocked: %#v", revoked)
	case assigned := <-peerAssignments:
		t.Fatalf("peer received an assignment while the first handler was still blocked: %#v", assigned)
	case <-timer.C:
	}
}

func waitForKafkaJSRuntimeGroupMembers(
	t *testing.T,
	ctx context.Context,
	broker string,
	group string,
	want int,
) {
	t.Helper()
	admin, err := confluentKafka.NewAdminClient(&confluentKafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	for {
		result, describeErr := admin.DescribeConsumerGroups(ctx, []string{group})
		if describeErr == nil && len(result.ConsumerGroupDescriptions) == 1 {
			description := result.ConsumerGroupDescriptions[0]
			if description.Error.Code() == confluentKafka.ErrNoError && len(description.Members) >= want {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("consumer group %s did not expose %d joined members: result=%#v err=%v", group, want, result, describeErr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func runKafkaJSUnresolvedMultiRecordFixture(t *testing.T, broker string, redeliver bool, want []int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "fit-go-unresolved-redelivery-" + suffix
	group := "fit-go-unresolved-redelivery-group-" + suffix

	admin, err := confluentKafka.NewAdminClient(&confluentKafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	results, err := admin.CreateTopics(ctx, []confluentKafka.TopicSpecification{{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}})
	if err != nil || len(results) != 1 || (results[0].Error.Code() != confluentKafka.ErrNoError && results[0].Error.Code() != confluentKafka.ErrTopicAlreadyExists) {
		t.Fatalf("create topic: results=%#v err=%v", results, err)
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Value: []byte("zero")},
		&kgo.Record{Topic: topic, Value: []byte("one")},
	).FirstErr(); err != nil {
		t.Fatalf("produce fixture: %v", err)
	}

	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	consumerConfig := DefaultConsumerConfig(group)
	consumerConfig.Backend = ConsumerBackendKafkaJSCompatible
	consumerConfig.AutoCommit = false
	consumer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "fit-go-redelivery-test"}, consumerConfig, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = consumer.Close() }()

	var deliveriesMu sync.Mutex
	deliveries := make([]int64, 0, len(want))
	completed := make(chan struct{}, 1)
	runDone := make(chan error, 1)
	handlerFailure := errors.New("caught task failure")
	manualCommit := false
	failedZero := false
	go func() {
		runDone <- consumer.ConsumeCtx(func(_ context.Context, payload MessagePayload) error {
			if payload.Offset < 0 || payload.Offset > 1 || string(payload.Value) != []string{"zero", "one"}[payload.Offset] {
				return fmt.Errorf("unexpected payload offset=%d value=%q", payload.Offset, payload.Value)
			}
			deliveriesMu.Lock()
			deliveries = append(deliveries, payload.Offset)
			isFirstZero := payload.Offset == 0 && !failedZero
			if isFirstZero {
				failedZero = true
			}
			if len(deliveries) == len(want) {
				select {
				case completed <- struct{}{}:
				default:
				}
			}
			deliveriesMu.Unlock()
			if isFirstZero {
				return handlerFailure
			}
			return nil
		}, ConsumerOptions{
			AutoCommit: &manualCommit,
			OffsetFinalizer: func(_ context.Context, _ MessagePayload, _ error, _ ExactOffsetCommit) error {
				return nil
			},
			ResolveAfterSuccessfulFinalizer: true,
			RedeliverUnresolvedFinalizer:    redeliver,
			PollTimeout:                     100 * time.Millisecond,
			MaxRecords:                      2,
		})
	}()

	select {
	case <-completed:
	case err := <-runDone:
		t.Fatalf("consumer ended before expected deliveries: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for same-process delivery sequence")
	}
	waitForKafkaJSCommittedOffset(t, ctx, broker, group, topic, 2)
	deliveriesMu.Lock()
	got := append([]int64(nil), deliveries...)
	deliveriesMu.Unlock()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("deliveries = %v, want %v", got, want)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("consumer shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

func waitForKafkaJSCommittedOffset(t *testing.T, ctx context.Context, broker, group, topic string, want int64) {
	t.Helper()
	_ = waitForKafkaJSCommittedTopicPartition(t, ctx, broker, group, topic, want)
}

func waitForKafkaJSCommittedTopicPartition(
	t *testing.T,
	ctx context.Context,
	broker string,
	group string,
	topic string,
	want int64,
) confluentKafka.TopicPartition {
	t.Helper()
	reader, err := confluentKafka.NewConsumer(&confluentKafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           group,
		"enable.auto.commit": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for {
		partitions, commitErr := reader.Committed([]confluentKafka.TopicPartition{{Topic: &topic, Partition: 0}}, 2000)
		if commitErr == nil && len(partitions) == 1 && int64(partitions[0].Offset) == want {
			return partitions[0]
		}
		select {
		case <-ctx.Done():
			t.Fatalf("committed offset did not reach %d: partitions=%#v err=%v", want, partitions, commitErr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
