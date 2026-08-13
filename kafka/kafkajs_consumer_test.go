package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/gofynd/fit-go/logging"
)

type fakeKafkaJSConsumerClient struct {
	mu sync.Mutex

	pollFn          func(context.Context, int) kgo.Fetches
	commitRecordsFn func(context.Context, ...*kgo.Record) error

	pollCalls           int
	allowRebalanceCalls int
	closeCalls          int
	markedOffsets       []int64
	exactOffsets        []int64
	setOffsetsCalls     int
}

func (f *fakeKafkaJSConsumerClient) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	f.mu.Lock()
	f.pollCalls++
	f.mu.Unlock()
	if f.pollFn != nil {
		return f.pollFn(ctx, maxRecords)
	}
	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (f *fakeKafkaJSConsumerClient) pollCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pollCalls
}

func (f *fakeKafkaJSConsumerClient) AllowRebalance() {
	f.mu.Lock()
	f.allowRebalanceCalls++
	f.mu.Unlock()
}

func (f *fakeKafkaJSConsumerClient) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	if f.commitRecordsFn != nil {
		return f.commitRecordsFn(ctx, records...)
	}
	f.mu.Lock()
	for _, record := range records {
		f.exactOffsets = append(f.exactOffsets, record.Offset+1)
	}
	f.mu.Unlock()
	return nil
}

func (f *fakeKafkaJSConsumerClient) MarkCommitRecords(records ...*kgo.Record) {
	f.mu.Lock()
	for _, record := range records {
		f.markedOffsets = append(f.markedOffsets, record.Offset+1)
	}
	f.mu.Unlock()
}

func (f *fakeKafkaJSConsumerClient) SetOffsets(map[string]map[int32]kgo.EpochOffset) {
	f.mu.Lock()
	f.setOffsetsCalls++
	f.mu.Unlock()
}

func (f *fakeKafkaJSConsumerClient) CommitOffsetsSync(
	_ context.Context,
	offsets map[string]map[int32]kgo.EpochOffset,
	onDone func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
) {
	f.mu.Lock()
	for _, partitions := range offsets {
		for _, offset := range partitions {
			f.exactOffsets = append(f.exactOffsets, offset.Offset)
		}
	}
	f.mu.Unlock()
	onDone(nil, nil, kmsg.NewPtrOffsetCommitResponse(), nil)
}

func (f *fakeKafkaJSConsumerClient) CloseAllowingRebalance() {
	f.mu.Lock()
	f.closeCalls++
	f.mu.Unlock()
}

func (f *fakeKafkaJSConsumerClient) snapshot() (allowRebalances, closes int, marked, exact []int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.allowRebalanceCalls, f.closeCalls,
		append([]int64(nil), f.markedOffsets...), append([]int64(nil), f.exactOffsets...)
}

func kafkaJSTestFetch(record *kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: record.Topic,
		Partitions: []kgo.FetchPartition{{
			Partition: record.Partition,
			Records:   []*kgo.Record{record},
		}},
	}}}}
}

func newKafkaJSLifecycleTestConsumer(t *testing.T, client kafkaJSConsumerClient, policy ConsumerShutdownPolicy) *kafkaJSCompatibleConsumer {
	t.Helper()
	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	return &kafkaJSCompatibleConsumer{
		client: client,
		config: ConsumerConfig{
			GroupID:        "kafkajs-shutdown-test",
			AutoCommit:     false,
			ShutdownPolicy: policy,
		},
		logger: logger,
	}
}

func TestKafkaJSTransientConsumerErrorClassification(t *testing.T) {
	networkCause := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{name: "dial failure", err: fmt.Errorf("post-handler commit failed: %w", networkCause), transient: true},
		{name: "retriable protocol failure", err: fmt.Errorf("commit failed: %w", kerr.CoordinatorNotAvailable), transient: true},
		{name: "unknown member rejoins", err: fmt.Errorf("join failed: %w", kerr.UnknownMemberID), transient: true},
		{name: "illegal generation rejoins", err: fmt.Errorf("heartbeat failed: %w", kerr.IllegalGeneration), transient: true},
		{name: "rebalance in progress rejoins", err: fmt.Errorf("fetch failed: %w", kerr.RebalanceInProgress), transient: true},
		{name: "permanent protocol failure", err: fmt.Errorf("commit failed: %w", kerr.GroupAuthorizationFailed), transient: false},
		{name: "invalid group remains permanent", err: fmt.Errorf("join failed: %w", kerr.InvalidGroupID), transient: false},
		{name: "incompatible protocol remains permanent", err: fmt.Errorf("join failed: %w", kerr.InconsistentGroupProtocol), transient: false},
		{name: "handler failure", err: errors.New("validation failed"), transient: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyKafkaJSTransientConsumerError(tc.err)
			isTransient := IsTransientConsumerError(got)
			if isTransient != tc.transient {
				t.Fatalf("IsTransientConsumerError(%v) = %v, want %v", got, isTransient, tc.transient)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("classified error no longer unwraps to original: %v", got)
			}
		})
	}
}

func TestKafkaJSGroupRejoinErrorsRestartFromConsumerPollBoundary(t *testing.T) {
	for _, groupErr := range []error{
		kerr.UnknownMemberID,
		kerr.IllegalGeneration,
		kerr.RebalanceInProgress,
	} {
		t.Run(groupErr.Error(), func(t *testing.T) {
			client := &fakeKafkaJSConsumerClient{pollFn: func(context.Context, int) kgo.Fetches {
				// This is the exact wrapper franz-go injects into PollRecords when a
				// member is kicked from, or cannot join, its consumer group.
				return kgo.NewErrFetch(&kgo.ErrGroupSession{Err: groupErr})
			}}
			consumer := newKafkaJSLifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			handlerCalled := false

			got := consumer.ConsumeCtx(func(context.Context, MessagePayload) error {
				handlerCalled = true
				return nil
			}, ConsumerOptions{PollTimeout: time.Second})

			if !IsTransientConsumerError(got) || !errors.Is(got, groupErr) {
				t.Fatalf("consume error = %v, want transient error preserving %v", got, groupErr)
			}
			if consumer.client != nil {
				t.Fatal("group-rejoin failure retained the stale consumer client")
			}
			allowRebalances, closes, _, _ := client.snapshot()
			if allowRebalances != 1 || closes != 1 {
				t.Fatalf("rebalance/close calls = %d/%d, want 1/1", allowRebalances, closes)
			}
			if handlerCalled {
				t.Fatal("consumer invoked the handler for a group-session error")
			}
		})
	}
}

func TestKafkaJSNullMetadataOffsetCommitHookPreservesRequest(t *testing.T) {
	metadata := "go-member"
	request := kmsg.NewPtrOffsetCommitRequest()
	request.Group = "legacy-basic-group-1"
	request.Generation = 7
	request.MemberID = metadata
	request.Topics = append(request.Topics, kmsg.OffsetCommitRequestTopic{
		Topic: "discount-events",
		Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
			Partition: 3, Offset: 42, LeaderEpoch: -1, Metadata: &metadata,
		}},
	})
	if err := clearKafkaJSOffsetCommitMetadata(request); err != nil {
		t.Fatal(err)
	}
	if request.Group != "legacy-basic-group-1" || request.MemberID != "go-member" || request.Generation != 7 {
		t.Fatalf("group identity changed: %#v", request)
	}
	partition := request.Topics[0].Partitions[0]
	if partition.Partition != 3 || partition.Offset != 42 || partition.LeaderEpoch != -1 {
		t.Fatalf("partition changed: %#v", partition)
	}
	if partition.Metadata != nil {
		t.Fatalf("metadata = %q, want null", *partition.Metadata)
	}
	if err := clearKafkaJSOffsetCommitMetadata(nil); err == nil {
		t.Fatal("nil request was accepted")
	}
}

func TestKafkaJSNullMetadataOffsetCommitResponseValidation(t *testing.T) {
	if err := kafkaJSOffsetCommitResponseError(nil); err == nil {
		t.Fatal("nil response was accepted")
	}
	response := kmsg.NewPtrOffsetCommitResponse()
	response.Topics = append(response.Topics, kmsg.OffsetCommitResponseTopic{
		Topic:      "discount-events",
		Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 3}},
	})
	if err := kafkaJSOffsetCommitResponseError(response); err != nil {
		t.Fatalf("successful response: %v", err)
	}

	t.Run("retriable coordinator error retains Kafka identity", func(t *testing.T) {
		response.Topics[0].Partitions[0].ErrorCode = int16(kerr.CoordinatorNotAvailable.Code)
		err := kafkaJSOffsetCommitResponseError(response)
		if !errors.Is(err, kerr.CoordinatorNotAvailable) {
			t.Fatalf("response error = %v, want CoordinatorNotAvailable identity", err)
		}
		if classified := classifyKafkaJSTransientConsumerError(err); !IsTransientConsumerError(classified) {
			t.Fatalf("coordinator error was not classified transient: %v", classified)
		}
	})

	t.Run("permanent authorization error retains Kafka identity", func(t *testing.T) {
		response.Topics[0].Partitions[0].ErrorCode = int16(kerr.GroupAuthorizationFailed.Code)
		err := kafkaJSOffsetCommitResponseError(response)
		if !errors.Is(err, kerr.GroupAuthorizationFailed) {
			t.Fatalf("response error = %v, want GroupAuthorizationFailed identity", err)
		}
		if classified := classifyKafkaJSTransientConsumerError(err); IsTransientConsumerError(classified) {
			t.Fatalf("authorization error was incorrectly classified transient: %v", classified)
		}
	})
}

func TestNewTransientConsumerErrorPreservesCauseAndIdentity(t *testing.T) {
	cause := errors.New("broker transport unavailable")
	marked := NewTransientConsumerError(cause)
	if !IsTransientConsumerError(marked) || !errors.Is(marked, cause) {
		t.Fatalf("marked error = %v, want transient wrapper preserving cause", marked)
	}
	if got := NewTransientConsumerError(marked); got != marked {
		t.Fatal("marking an existing transient error changed its identity")
	}
	if got := NewTransientConsumerError(nil); got != nil {
		t.Fatalf("nil cause = %v, want nil", got)
	}
}

func TestKafkaJSTransientRunFailureRebuildsClientFromCommittedBoundary(t *testing.T) {
	logger, err := logging.New(logging.Options{Level: "info"})
	if err != nil {
		t.Fatal(err)
	}
	consumer := &kafkaJSCompatibleConsumer{
		brokers: []string{"127.0.0.1:1"},
		fitCfg:  &Config{ClientID: "test"},
		config:  ConsumerConfig{GroupID: "group", AutoCommit: false},
		logger:  logger,
		topics:  []TopicConfig{{Topic: "discounts"}},
	}
	opts, err := consumer.clientOptions(consumer.topics)
	if err != nil {
		t.Fatal(err)
	}
	original, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	consumer.client = original

	cause := errors.New("broker transport unavailable")
	marked := NewTransientConsumerError(cause)
	if got := consumer.prepareTransientRunRetry(original, marked); got != marked {
		t.Fatalf("prepare error = %v, want original transient wrapper", got)
	}
	if consumer.client != nil {
		t.Fatal("transient run failure retained the advanced franz-go client")
	}

	rebuilt, _, _, finish, err := consumer.beginRun()
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt == nil || rebuilt == original || consumer.client != rebuilt {
		t.Fatalf("rebuilt client = %p, original = %p, stored = %p", rebuilt, original, consumer.client)
	}
	finish()
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaJSPermanentRunFailureRetainsClient(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	consumer := &kafkaJSCompatibleConsumer{client: client}
	want := errors.New("handler rejected message")
	if got := consumer.prepareTransientRunRetry(client, want); got != want {
		t.Fatalf("prepare error = %v, want permanent error identity", got)
	}
	if consumer.client != client {
		t.Fatal("permanent failure discarded a healthy client")
	}
}

func TestKafkaJSRoundRobinBalancerChangesOnlyProtocolName(t *testing.T) {
	base := kgo.RoundRobinBalancer()
	candidate := kafkaJSRoundRobinBalancer{GroupBalancer: base}
	if got := candidate.ProtocolName(); got != "RoundRobinAssigner" {
		t.Fatalf("protocol name = %q", got)
	}
	interests := []string{"discounts"}
	assignment := map[string][]int32{"discounts": {0, 1}}
	if got, want := candidate.JoinGroupMetadata(interests, assignment, 7), base.JoinGroupMetadata(interests, assignment, 7); !reflect.DeepEqual(got, want) {
		t.Fatal("KafkaJS wrapper changed standard consumer-group metadata")
	}
	if candidate.IsCooperative() != base.IsCooperative() {
		t.Fatal("KafkaJS wrapper changed eager/cooperative semantics")
	}
}

func TestKafkaJSRoundRobinBalancerMatchesMultiMemberMultiPartitionPlan(t *testing.T) {
	base := kgo.RoundRobinBalancer()
	candidate := kafkaJSRoundRobinBalancer{GroupBalancer: base}
	members := []kmsg.JoinGroupResponseMember{
		{MemberID: "legacy-kafkajs", ProtocolMetadata: base.JoinGroupMetadata([]string{"discount-a", "discount-b"}, nil, 1)},
		{MemberID: "go-client", ProtocolMetadata: candidate.JoinGroupMetadata([]string{"discount-a", "discount-b"}, nil, 1)},
	}
	baseBalancer, baseTopics, err := base.MemberBalancer(members)
	if err != nil {
		t.Fatal(err)
	}
	candidateBalancer, candidateTopics, err := candidate.MemberBalancer(members)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(candidateTopics, baseTopics) {
		t.Fatalf("topics = %#v, want %#v", candidateTopics, baseTopics)
	}
	partitions := map[string]int32{"discount-a": 3, "discount-b": 2}
	basePlan, err := baseBalancer.(interface {
		BalanceOrError(map[string]int32) (kgo.IntoSyncAssignment, error)
	}).BalanceOrError(partitions)
	if err != nil {
		t.Fatal(err)
	}
	candidatePlan, err := candidateBalancer.(interface {
		BalanceOrError(map[string]int32) (kgo.IntoSyncAssignment, error)
	}).BalanceOrError(partitions)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := candidatePlan.IntoSyncAssignment(), basePlan.IntoSyncAssignment(); !reflect.DeepEqual(got, want) {
		t.Fatalf("assignment = %#v, want %#v", got, want)
	}
}

func TestKafkaJSCompatibleManualConsumerDisablesBackgroundAutoCommit(t *testing.T) {
	logger, err := logging.New(logging.Options{Level: "info"})
	if err != nil {
		t.Fatal(err)
	}
	consumer := &kafkaJSCompatibleConsumer{
		brokers: []string{"127.0.0.1:1"},
		fitCfg:  &Config{ClientID: "test"},
		config:  ConsumerConfig{GroupID: "group", AutoCommit: false},
		logger:  logger,
	}
	opts, err := consumer.clientOptions([]TopicConfig{{Topic: "discounts"}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if disabled, ok := client.OptValue(kgo.DisableAutoCommit).(bool); !ok || !disabled {
		t.Fatalf("DisableAutoCommit = %#v; manual mode could background-commit an unhandled record", client.OptValue(kgo.DisableAutoCommit))
	}
}

func TestKafkaJSCompatibleConsumerAppliesOptInTransportCompatibility(t *testing.T) {
	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	consumer := &kafkaJSCompatibleConsumer{
		brokers: []string{"127.0.0.1:1"},
		fitCfg:  &Config{ClientID: "mixmaster-server"},
		config: ConsumerConfig{
			GroupID:                "mixmaster-consumer-common-group-1",
			AutoCommit:             false,
			ReadCommitted:          true,
			AutoCreateTopics:       true,
			DialTimeout:            time.Second,
			RequestRetries:         5,
			RetryBackoff:           300 * time.Millisecond,
			RetryBackoffMax:        30 * time.Second,
			RetryBackoffMultiplier: 2,
			RetryBackoffFactor:     0.2,
		},
		logger: logger,
	}
	opts, err := consumer.clientOptions([]TopicConfig{{Topic: "background-task"}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if got := client.OptValue(kgo.FetchIsolationLevel); got != int8(1) {
		t.Fatalf("FetchIsolationLevel = %#v, want ReadCommitted", got)
	}
	if got := client.OptValue(kgo.AllowAutoTopicCreation); got != true {
		t.Fatalf("AllowAutoTopicCreation = %#v, want true", got)
	}
	if got := client.OptValue(kgo.DialTimeout); got != time.Second {
		t.Fatalf("DialTimeout = %#v, want 1s", got)
	}
	if got := client.OptValue(kgo.RequestRetries); got != int64(5) {
		t.Fatalf("RequestRetries = %#v, want 5", got)
	}
	backoff, ok := client.OptValue(kgo.RetryBackoffFn).(func(int) time.Duration)
	if !ok {
		t.Fatalf("RetryBackoffFn = %#v", client.OptValue(kgo.RetryBackoffFn))
	}
	for attempt, bounds := range [][2]time.Duration{
		{240 * time.Millisecond, 360 * time.Millisecond},
		{480 * time.Millisecond, 720 * time.Millisecond},
		{960 * time.Millisecond, 1440 * time.Millisecond},
	} {
		got := backoff(attempt)
		if got < bounds[0] || got > bounds[1] {
			t.Fatalf("backoff(%d) = %s, want [%s,%s]", attempt, got, bounds[0], bounds[1])
		}
	}
}

func TestKafkaJSCompatibleStandardCommitCanClearMetadata(t *testing.T) {
	client := &fakeKafkaJSConsumerClient{}
	consumer := &kafkaJSCompatibleConsumer{config: ConsumerConfig{GroupID: "group", AutoCommit: false}}
	record := &kgo.Record{Topic: "topic", Partition: 2, Offset: 17}
	err := consumer.processRecord(
		context.Background(),
		client,
		record,
		func(context.Context, MessagePayload) error { return nil },
		false,
		ConsumerOptions{NullOffsetCommitMetadata: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, exact := client.snapshot()
	if want := []int64{18}; !reflect.DeepEqual(exact, want) {
		t.Fatalf("committed offsets = %v, want %v", exact, want)
	}
}

func TestKafkaJSCompatibleConsumerKeepsRebalanceTimeoutIndependentFromMaxPollInterval(t *testing.T) {
	logger, err := logging.New(logging.Options{Level: "info"})
	if err != nil {
		t.Fatal(err)
	}
	wantRebalanceTimeout := 73 * time.Second
	consumer := &kafkaJSCompatibleConsumer{
		brokers: []string{"127.0.0.1:1"},
		fitCfg:  &Config{ClientID: "test"},
		config: ConsumerConfig{
			GroupID:          "group",
			AutoCommit:       false,
			RebalanceTimeout: wantRebalanceTimeout,
			MaxPollInterval:  17 * time.Minute,
		},
		logger: logger,
	}
	opts, err := consumer.clientOptions([]TopicConfig{{Topic: "discounts"}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := client.OptValue(kgo.RebalanceTimeout); got != wantRebalanceTimeout {
		t.Fatalf("RebalanceTimeout = %#v, want %v; MaxPollInterval must not alter the KafkaJS JoinGroup timeout", got, wantRebalanceTimeout)
	}
}

func TestKafkaJSCompatibleCloseWaitsForRunAndConcurrentCallers(t *testing.T) {
	logger, err := logging.New(logging.Options{Level: "info"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	consumer := &kafkaJSCompatibleConsumer{
		client:    client,
		cancelRun: cancelRun,
		runDone:   runDone,
		config:    ConsumerConfig{GroupID: "group"},
		logger:    logger,
	}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- consumer.Close() }()
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the active run")
	}
	select {
	case err := <-first:
		t.Fatalf("Close returned before the active run drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	go func() { second <- consumer.Close() }()
	select {
	case err := <-second:
		t.Fatalf("concurrent Close returned before the first close completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(runDone)
	for name, result := range map[string]<-chan error{"first": first, "second": second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s Close: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s Close did not complete after the run drained", name)
		}
	}
}

func TestKafkaJSCompatibleDefaultShutdownCancelsInFlightHandler(t *testing.T) {
	record := &kgo.Record{Topic: "discounts", Partition: 0, Offset: 7, Value: []byte("work")}
	var pollOnce sync.Once
	client := &fakeKafkaJSConsumerClient{}
	client.pollFn = func(ctx context.Context, _ int) kgo.Fetches {
		var fetches kgo.Fetches
		pollOnce.Do(func() { fetches = kafkaJSTestFetch(record) })
		if fetches != nil {
			return fetches
		}
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	consumer := newKafkaJSLifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)

	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.ConsumeCtx(func(ctx context.Context, _ MessagePayload) error {
			close(handlerStarted)
			<-ctx.Done()
			close(handlerCanceled)
			return ctx.Err()
		}, ConsumerOptions{PollTimeout: time.Second, MaxRecords: 1})
	}()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- consumer.Close() }()
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("default shutdown did not cancel the in-flight handler")
	}
	select {
	case err := <-consumeDone:
		if err != nil {
			t.Fatalf("ConsumeCtx: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consume run did not stop after handler cancellation")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}

	allowRebalances, closes, _, exact := client.snapshot()
	if allowRebalances == 0 {
		t.Fatal("shutdown did not release BlockRebalanceOnPoll")
	}
	if closes != 1 {
		t.Fatalf("client closes = %d, want 1", closes)
	}
	if len(exact) != 0 {
		t.Fatalf("canceled record committed offsets %v", exact)
	}
}

func TestKafkaJSCompatibleDrainShutdownCompletesMarkerFinalizerAndCommits(t *testing.T) {
	record := &kgo.Record{Topic: "discounts", Partition: 2, Offset: 17, Value: []byte("work")}
	var pollOnce sync.Once
	client := &fakeKafkaJSConsumerClient{}
	client.pollFn = func(ctx context.Context, _ int) kgo.Fetches {
		var fetches kgo.Fetches
		pollOnce.Do(func() { fetches = kafkaJSTestFetch(record) })
		if fetches != nil {
			return fetches
		}
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	consumer := newKafkaJSLifecycleTestConsumer(t, client, ConsumerShutdownDrainInFlight)

	markerWritten := make(chan struct{})
	handlerCanceled := make(chan struct{})
	releaseHandler := make(chan struct{})
	finalizerFinished := make(chan struct{})
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.ConsumeCtx(func(ctx context.Context, _ MessagePayload) error {
			// Models a legacy consumer's Redis SETEX marker-before-work boundary.
			close(markerWritten)
			select {
			case <-ctx.Done():
				close(handlerCanceled)
				return ctx.Err()
			case <-releaseHandler:
				return nil
			}
		}, ConsumerOptions{
			PollTimeout: time.Second,
			MaxRecords:  1,
			OffsetFinalizer: func(_ context.Context, payload MessagePayload, handlerErr error, commit ExactOffsetCommit) error {
				if handlerErr != nil {
					return handlerErr
				}
				if err := commit(payload.Offset); err != nil {
					return err
				}
				close(finalizerFinished)
				return nil
			},
			ResolveAfterSuccessfulFinalizer: true,
			NullOffsetCommitMetadata:        true,
		})
	}()

	select {
	case <-markerWritten:
	case <-time.After(time.Second):
		t.Fatal("handler did not reach the marker boundary")
	}
	firstClose := make(chan error, 1)
	secondClose := make(chan error, 1)
	go func() { firstClose <- consumer.Close() }()
	go func() { secondClose <- consumer.Close() }()
	select {
	case <-handlerCanceled:
		t.Fatal("drain shutdown canceled admitted handler work")
	case err := <-firstClose:
		t.Fatalf("Close returned before admitted work completed: %v", err)
	case err := <-secondClose:
		t.Fatalf("concurrent Close returned before admitted work completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseHandler)
	select {
	case <-finalizerFinished:
	case <-time.After(time.Second):
		t.Fatal("drain shutdown did not finish the finalizer commit")
	}
	for name, result := range map[string]<-chan error{"first": firstClose, "second": secondClose, "consume": consumeDone} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s result: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}

	allowRebalances, closes, _, exact := client.snapshot()
	if allowRebalances == 0 {
		t.Fatal("drain shutdown did not release BlockRebalanceOnPoll")
	}
	if closes != 1 {
		t.Fatalf("client closes = %d, want 1", closes)
	}
	if want := []int64{17, 18}; !reflect.DeepEqual(exact, want) {
		t.Fatalf("exact offset commits = %v, want %v", exact, want)
	}
	if polls := client.pollCount(); polls != 1 {
		t.Fatalf("drain shutdown poll calls = %d, want 1; shutdown admitted another poll", polls)
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	_, closes, _, _ = client.snapshot()
	if closes != 1 {
		t.Fatalf("repeated Close closed client %d times, want 1", closes)
	}
}

func TestKafkaJSCompatibleDrainShutdownDoesNotHangWhileIdle(t *testing.T) {
	pollStarted := make(chan struct{})
	var pollStartedOnce sync.Once
	client := &fakeKafkaJSConsumerClient{}
	client.pollFn = func(ctx context.Context, _ int) kgo.Fetches {
		pollStartedOnce.Do(func() { close(pollStarted) })
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	consumer := newKafkaJSLifecycleTestConsumer(t, client, ConsumerShutdownDrainInFlight)

	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- consumer.ConsumeCtx(func(context.Context, MessagePayload) error {
			t.Error("idle consumer unexpectedly invoked its handler")
			return nil
		}, ConsumerOptions{PollTimeout: time.Hour, MaxRecords: 1})
	}()
	select {
	case <-pollStarted:
	case <-time.After(time.Second):
		t.Fatal("consumer did not enter PollRecords")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- consumer.Close() }()
	for name, result := range map[string]<-chan error{"close": closeDone, "consume": consumeDone} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s result: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s hung while the consumer was idle", name)
		}
	}
	allowRebalances, closes, _, _ := client.snapshot()
	if allowRebalances == 0 || closes != 1 {
		t.Fatalf("shutdown lifecycle = allowRebalance %d, closes %d; want >=1/1", allowRebalances, closes)
	}
}

func TestKafkaJSCompatibleShutdownHandlesRecordReturnedByActivePollPerPolicy(t *testing.T) {
	tests := []struct {
		name       string
		policy     ConsumerShutdownPolicy
		wantHandle bool
		wantCommit []int64
	}{
		{
			name:   "default cancellation leaves record replayable",
			policy: ConsumerShutdownCancelInFlight,
		},
		{
			name:       "drain completes record from already active poll",
			policy:     ConsumerShutdownDrainInFlight,
			wantHandle: true,
			wantCommit: []int64{10},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := &kgo.Record{Topic: "discounts", Partition: 0, Offset: 9, Value: []byte("buffered")}
			pollStarted := make(chan struct{})
			var pollOnce sync.Once
			client := &fakeKafkaJSConsumerClient{}
			client.pollFn = func(ctx context.Context, _ int) kgo.Fetches {
				var fetches kgo.Fetches
				pollOnce.Do(func() {
					close(pollStarted)
					<-ctx.Done()
					fetches = kafkaJSTestFetch(record)
				})
				if fetches != nil {
					return fetches
				}
				<-ctx.Done()
				return kgo.NewErrFetch(ctx.Err())
			}
			consumer := newKafkaJSLifecycleTestConsumer(t, client, test.policy)
			handled := make(chan struct{})
			consumeDone := make(chan error, 1)
			go func() {
				consumeDone <- consumer.ConsumeCtx(func(context.Context, MessagePayload) error {
					close(handled)
					return nil
				}, ConsumerOptions{PollTimeout: time.Hour, MaxRecords: 1})
			}()
			select {
			case <-pollStarted:
			case <-time.After(time.Second):
				t.Fatal("consumer did not start its first poll")
			}

			if err := consumer.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			select {
			case err := <-consumeDone:
				if err != nil {
					t.Fatalf("ConsumeCtx: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("consume run did not finish")
			}

			wasHandled := false
			select {
			case <-handled:
				wasHandled = true
			default:
			}
			if wasHandled != test.wantHandle {
				t.Fatalf("handler called = %v, want %v", wasHandled, test.wantHandle)
			}
			_, _, _, exact := client.snapshot()
			if !reflect.DeepEqual(exact, test.wantCommit) {
				t.Fatalf("committed offsets = %v, want %v", exact, test.wantCommit)
			}
			if polls := client.pollCount(); polls != 1 {
				t.Fatalf("poll calls = %d, want 1", polls)
			}
		})
	}
}

func TestKafkaJSCompatibleRejectsUnknownShutdownPolicy(t *testing.T) {
	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = newKafkaJSCompatibleConsumer(
		[]string{"127.0.0.1:1"},
		&Config{ClientID: "test"},
		ConsumerConfig{GroupID: "group", ShutdownPolicy: ConsumerShutdownPolicy(255)},
		logger,
	)
	if err == nil || !strings.Contains(err.Error(), "unsupported shutdown policy") {
		t.Fatalf("unknown shutdown policy error = %v", err)
	}
}

func TestKafkaJSRunCancellationDoesNotHideConcurrentProcessingFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if !isKafkaJSRunCancellation(ctx, fmt.Errorf("handler stopped: %w", context.Canceled)) {
		t.Fatal("wrapped run cancellation must be treated as a clean shutdown")
	}
	if isKafkaJSRunCancellation(ctx, errors.New("database write failed")) {
		t.Fatal("a real processing failure racing shutdown must remain visible")
	}
	if isKafkaJSRunCancellation(context.Background(), context.Canceled) {
		t.Fatal("a cancellation error without a canceled run must remain visible")
	}
}

func TestConfluentClientSelectsKafkaJSConsumerOnlyWhenExplicit(t *testing.T) {
	client, err := NewConfluentClient(&Config{Brokers: []string{"127.0.0.1:9092"}, ClientID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	legacyCompatible, err := client.Consumer(ConsumerConfig{
		GroupID: "group",
		Backend: ConsumerBackendKafkaJSCompatible,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := legacyCompatible.(*kafkaJSCompatibleConsumer); !ok {
		t.Fatalf("explicit backend produced %T", legacyCompatible)
	}

	standard, err := client.Consumer(ConsumerConfig{GroupID: "group"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := standard.(*ConfluentConsumer); !ok {
		t.Fatalf("zero-value backend produced %T", standard)
	}

	if _, err = client.Consumer(ConsumerConfig{GroupID: "group", Backend: ConsumerBackend(255)}); err == nil {
		t.Fatal("unknown backend was accepted")
	}
}

func TestKafkaJSCompatibleSASLRejectsUnknownMechanism(t *testing.T) {
	if _, err := kafkaJSCompatibleSASL(&SASLConfig{Mechanism: "GSSAPI"}); err == nil {
		t.Fatal("unsupported SASL mechanism was accepted")
	}
	for _, mechanism := range []string{"", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		if _, err := kafkaJSCompatibleSASL(&SASLConfig{Mechanism: mechanism, Username: "u", Password: "p"}); err != nil {
			t.Fatalf("mechanism %q: %v", mechanism, err)
		}
	}
}

func TestFlattenAssignmentsIsStable(t *testing.T) {
	got := flattenAssignments(map[string][]int32{"z": {2, 0}, "a": {3, 1}})
	want := []PartitionAssignment{{Topic: "a", Partition: 1}, {Topic: "a", Partition: 3}, {Topic: "z", Partition: 0}, {Topic: "z", Partition: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assignments = %#v", got)
	}
}
