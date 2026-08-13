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

package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type fakeConfluentProducerDriver struct {
	produceFn      func(*ckafka.Message, chan ckafka.Event) error
	flushFn        func(int) int
	closeFn        func()
	flushRemaining int

	mu          sync.Mutex
	flushCalls  int
	closeCalls  int
	closeCalled chan struct{}
}

type fakeMetadataConfluentProducerDriver struct {
	*fakeConfluentProducerDriver
	metadataFn func(*string, bool, int) (*ckafka.Metadata, error)
}

func (f *fakeMetadataConfluentProducerDriver) GetMetadata(
	topic *string,
	allTopics bool,
	timeoutMs int,
) (*ckafka.Metadata, error) {
	return f.metadataFn(topic, allTopics, timeoutMs)
}

func (f *fakeConfluentProducerDriver) Produce(message *ckafka.Message, deliveryChan chan ckafka.Event) error {
	if f.produceFn == nil {
		deliveryChan <- successfulDelivery(message, 0)
		return nil
	}
	return f.produceFn(message, deliveryChan)
}

func (f *fakeConfluentProducerDriver) Flush(timeoutMs int) int {
	f.mu.Lock()
	f.flushCalls++
	flushFn := f.flushFn
	f.mu.Unlock()
	if flushFn != nil {
		return flushFn(timeoutMs)
	}
	return f.flushRemaining
}

func (f *fakeConfluentProducerDriver) Close() {
	f.mu.Lock()
	f.closeCalls++
	closeCalled := f.closeCalled
	closeFn := f.closeFn
	f.mu.Unlock()
	if closeFn != nil {
		closeFn()
	}
	if closeCalled != nil {
		close(closeCalled)
	}
}

func (f *fakeConfluentProducerDriver) calls() (flush, close int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushCalls, f.closeCalls
}

func newTestConfluentProducer(driver confluentProducerDriver) *ConfluentProducer {
	return &ConfluentProducer{
		configMap:      &ckafka.ConfigMap{},
		logger:         mustLogger(),
		configuredAcks: -1,
		producer:       driver,
		producers:      map[int]confluentProducerDriver{-1: driver},
	}
}

func successfulDelivery(message *ckafka.Message, offset int64) *ckafka.Message {
	topic := ""
	partition := int32(0)
	if message != nil {
		if message.TopicPartition.Topic != nil {
			topic = *message.TopicPartition.Topic
		}
		partition = message.TopicPartition.Partition
	}
	return &ckafka.Message{TopicPartition: ckafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    ckafka.Offset(offset),
	}}
}

func waitForConfluentPendingReports(t *testing.T, producer *ConfluentProducer, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := producer.pendingReports.Load(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending delivery reports = %d, want %d", producer.pendingReports.Load(), want)
}

func metadataForTopic(topic string, partitions ...ckafka.PartitionMetadata) *ckafka.Metadata {
	return &ckafka.Metadata{Topics: map[string]ckafka.TopicMetadata{
		topic: {
			Topic:      topic,
			Partitions: partitions,
		},
	}}
}

func TestConfluentProducerKafkaJSLegacyKeylessRoundRobin(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	var producedPartitions []int32
	base.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		producedPartitions = append(producedPartitions, message.TopicPartition.Partition)
		reports <- successfulDelivery(message, int64(len(producedPartitions)))
		return nil
	}
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, allTopics bool, timeoutMs int) (*ckafka.Metadata, error) {
			if topic == nil || *topic != "events" {
				t.Fatalf("metadata topic = %v, want events", topic)
			}
			if allTopics {
				t.Fatal("allTopics = true, want false")
			}
			if timeoutMs != int(defaultProducerMetadataTimeout.Milliseconds()) {
				t.Fatalf("metadata timeout = %d, want %d", timeoutMs, defaultProducerMetadataTimeout.Milliseconds())
			}
			return &ckafka.Metadata{Topics: map[string]ckafka.TopicMetadata{
				"events": {
					Topic: "events",
					Partitions: []ckafka.PartitionMetadata{
						{ID: 0, Leader: -1},
						{ID: 1, Leader: 10},
						{ID: 2, Leader: 11},
					},
				},
			}}, nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible
	producer.partitionSeed = func() (uint32, error) { return 0, nil }

	err := producer.Produce("events", []Message{
		{Value: []byte("first"), Partition: -1},
		{Value: []byte("second"), Partition: -1},
		{Value: []byte("third"), Partition: -1},
	}, -1)
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	want := []int32{2, 1, 2}
	if fmt.Sprint(producedPartitions) != fmt.Sprint(want) {
		t.Fatalf("partitions = %v, want KafkaJS available-partition round robin %v", producedPartitions, want)
	}
}

func TestConfluentProducerKafkaJSLegacyCachesMetadataPerTopic(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	metadataCalls := make(map[string]int)
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, _ bool, _ int) (*ckafka.Metadata, error) {
			metadataCalls[*topic]++
			return metadataForTopic(*topic, ckafka.PartitionMetadata{ID: 0, Leader: 10}), nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible
	producer.partitionSeed = func() (uint32, error) { return 0, nil }

	for range 3 {
		if err := producer.Produce("events", []Message{NewMessage([]byte("value"))}, -1); err != nil {
			t.Fatalf("Produce(events) error = %v", err)
		}
	}
	if err := producer.Produce("audit", []Message{NewMessage([]byte("value"))}, -1); err != nil {
		t.Fatalf("Produce(audit) error = %v", err)
	}
	if metadataCalls["events"] != 1 || metadataCalls["audit"] != 1 {
		t.Fatalf("metadata calls = %#v, want one lookup per topic", metadataCalls)
	}
}

func TestConfluentProducerKafkaJSLegacyMetadataExpiresAtKafkaJSMaxAge(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	metadataCalls := 0
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, _ bool, _ int) (*ckafka.Metadata, error) {
			metadataCalls++
			return metadataForTopic(*topic, ckafka.PartitionMetadata{ID: 0, Leader: 10}), nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible
	producer.partitionSeed = func() (uint32, error) { return 0, nil }
	clock := time.Date(2026, time.August, 5, 0, 0, 0, 0, time.UTC)
	producer.metadataNow = func() time.Time { return clock }

	produce := func() {
		t.Helper()
		if err := producer.Produce("events", []Message{NewMessage([]byte("value"))}, -1); err != nil {
			t.Fatalf("Produce() error = %v", err)
		}
	}
	produce()
	clock = clock.Add(defaultProducerMetadataMaxAge - time.Nanosecond)
	produce()
	if metadataCalls != 1 {
		t.Fatalf("metadata calls before expiry = %d, want 1", metadataCalls)
	}
	clock = clock.Add(2 * time.Nanosecond)
	produce()
	if metadataCalls != 2 {
		t.Fatalf("metadata calls after expiry = %d, want 2", metadataCalls)
	}
}

func TestConfluentProducerKafkaJSLegacyRefreshFailurePreservesLastHealthyCacheEntry(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	metadataCalls := 0
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, _ bool, _ int) (*ckafka.Metadata, error) {
			metadataCalls++
			if metadataCalls == 2 {
				return nil, errors.New("metadata temporarily unavailable")
			}
			return metadataForTopic(*topic, ckafka.PartitionMetadata{ID: 0, Leader: 10}), nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible
	producer.partitionSeed = func() (uint32, error) { return 0, nil }
	clock := time.Date(2026, time.August, 5, 0, 0, 0, 0, time.UTC)
	producer.metadataNow = func() time.Time { return clock }

	if err := producer.Produce("events", []Message{NewMessage([]byte("first"))}, -1); err != nil {
		t.Fatalf("initial Produce() error = %v", err)
	}
	producer.metadataMu.Lock()
	wantCached := clonePartitionMetadata(producer.metadataCache["events"].partitions)
	producer.metadataMu.Unlock()
	clock = clock.Add(defaultProducerMetadataMaxAge)
	if err := producer.Produce("events", []Message{NewMessage([]byte("refresh"))}, -1); err == nil {
		t.Fatal("expired cache refresh succeeded, want metadata failure")
	}
	producer.metadataMu.Lock()
	gotCached := clonePartitionMetadata(producer.metadataCache["events"].partitions)
	producer.metadataMu.Unlock()
	if fmt.Sprint(gotCached) != fmt.Sprint(wantCached) {
		t.Fatalf("cache after refresh failure = %v, want last healthy %v", gotCached, wantCached)
	}
	if err := producer.Produce("events", []Message{NewMessage([]byte("retry"))}, -1); err != nil {
		t.Fatalf("retry Produce() error = %v", err)
	}
	if metadataCalls != 3 {
		t.Fatalf("metadata calls = %d, want initial, failed refresh, successful retry", metadataCalls)
	}
}

func TestConfluentProducerKafkaJSLegacyCoalescesConcurrentMetadataRefresh(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	metadataStarted := make(chan struct{})
	releaseMetadata := make(chan struct{})
	var startOnce sync.Once
	metadataCalls := 0
	var callsMu sync.Mutex
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, _ bool, _ int) (*ckafka.Metadata, error) {
			callsMu.Lock()
			metadataCalls++
			callsMu.Unlock()
			startOnce.Do(func() { close(metadataStarted) })
			<-releaseMetadata
			return metadataForTopic(*topic, ckafka.PartitionMetadata{ID: 0, Leader: 10}), nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible

	const callers = 24
	start := make(chan struct{})
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			_, err := producer.kafkaJSPartitionMetadata(context.Background(), driver, "events")
			errs <- err
		}()
	}
	close(start)
	<-metadataStarted
	close(releaseMetadata)
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("metadata lookup error = %v", err)
		}
	}
	callsMu.Lock()
	gotCalls := metadataCalls
	callsMu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("metadata calls = %d, want one coalesced lookup", gotCalls)
	}
}

func TestConfluentProducerKafkaJSLegacyMetadataWaitHonorsContext(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	metadataStarted := make(chan struct{})
	releaseMetadata := make(chan struct{})
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, _ bool, _ int) (*ckafka.Metadata, error) {
			close(metadataStarted)
			<-releaseMetadata
			return metadataForTopic(*topic, ckafka.PartitionMetadata{ID: 0, Leader: 10}), nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible
	ownerDone := make(chan error, 1)
	go func() {
		_, err := producer.kafkaJSPartitionMetadata(context.Background(), driver, "events")
		ownerDone <- err
	}()
	<-metadataStarted

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := producer.kafkaJSPartitionMetadata(ctx, driver, "events"); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting metadata error = %v, want context.Canceled", err)
	}
	close(releaseMetadata)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner metadata lookup error = %v", err)
	}
}

func TestConfluentProducerKafkaJSLegacyBatchReusesMetadataForDuplicateTopic(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	metadataCalls := 0
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, _ bool, _ int) (*ckafka.Metadata, error) {
			metadataCalls++
			return metadataForTopic(*topic, ckafka.PartitionMetadata{ID: 0, Leader: 10}), nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible
	producer.partitionSeed = func() (uint32, error) { return 0, nil }

	err := producer.ProduceBatch([]TopicMessages{
		{Topic: "events", Messages: []Message{NewMessage([]byte("one"))}},
		{Topic: "events", Messages: []Message{NewMessage([]byte("two"))}},
	}, -1)
	if err != nil {
		t.Fatalf("ProduceBatch() error = %v", err)
	}
	if metadataCalls != 1 {
		t.Fatalf("metadata calls = %d, want 1", metadataCalls)
	}
}

func TestConfluentProducerKafkaJSLegacyInvalidatesMetadataAfterTopologyError(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	metadataCalls := 0
	produceCalls := 0
	base.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		produceCalls++
		delivery := successfulDelivery(message, int64(produceCalls))
		if produceCalls == 1 {
			delivery.TopicPartition.Error = ckafka.NewError(ckafka.ErrNotLeaderForPartition, "leader moved", false)
		}
		reports <- delivery
		return nil
	}
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, _ bool, _ int) (*ckafka.Metadata, error) {
			metadataCalls++
			return metadataForTopic(*topic, ckafka.PartitionMetadata{ID: 0, Leader: 10}), nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible
	producer.partitionSeed = func() (uint32, error) { return 0, nil }

	if err := producer.Produce("events", []Message{NewMessage([]byte("first"))}, -1); err == nil {
		t.Fatal("first Produce() succeeded, want topology delivery failure")
	}
	if err := producer.Produce("events", []Message{NewMessage([]byte("second"))}, -1); err != nil {
		t.Fatalf("second Produce() error = %v", err)
	}
	if metadataCalls != 2 {
		t.Fatalf("metadata calls = %d, want refresh after topology error", metadataCalls)
	}
}

func TestConfluentProducerKafkaJSLegacySkipsMetadataForKeyedAndExplicitMessages(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	driver := &fakeMetadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(*string, bool, int) (*ckafka.Metadata, error) {
			t.Fatal("GetMetadata called for keyed or explicitly partitioned messages")
			return nil, nil
		},
	}
	producer := newTestConfluentProducer(driver)
	producer.partitioner = ProducerPartitionerKafkaJSCompatible

	if err := producer.Produce("events", []Message{
		NewKeyedMessage([]byte("key"), []byte("keyed")),
		NewPartitionedMessage(0, []byte("explicit")),
	}, -1); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
}

func TestConfluentProducerKafkaJSLegacyNoLeaderFallback(t *testing.T) {
	producer := &ConfluentProducer{
		partitionCounters: map[string]uint32{},
		partitionSeed:     func() (uint32, error) { return 0x7fffffff, nil },
	}
	metadata := []ckafka.PartitionMetadata{
		{ID: 7, Leader: -1},
		{ID: 9, Leader: -1},
		{ID: 12, Leader: -1},
	}

	first, err := producer.nextKafkaJSKeylessPartition("events", metadata)
	if err != nil {
		t.Fatalf("first partition error = %v", err)
	}
	second, err := producer.nextKafkaJSKeylessPartition("events", metadata)
	if err != nil {
		t.Fatalf("second partition error = %v", err)
	}
	if first != 0 || second != 1 {
		t.Fatalf("fallback partitions = [%d %d], want KafkaJS array indexes [0 1]", first, second)
	}
}

func TestConfluentProducerDrainsAcceptedReportsAfterDeliveryFailure(t *testing.T) {
	deliveryErr := errors.New("first delivery failed")
	firstReported := make(chan struct{})
	releaseSecond := make(chan struct{})
	secondReported := make(chan struct{})

	var deliveryChan chan ckafka.Event
	produceCalls := 0
	driver := &fakeConfluentProducerDriver{}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		produceCalls++
		deliveryChan = reports
		switch produceCalls {
		case 1:
			failed := successfulDelivery(message, 1)
			failed.TopicPartition.Error = deliveryErr
			reports <- failed
			close(firstReported)
		case 2:
			go func() {
				<-releaseSecond
				reports <- successfulDelivery(message, 2)
				close(secondReported)
			}()
		default:
			return fmt.Errorf("unexpected Produce call %d", produceCalls)
		}
		return nil
	}

	producer := newTestConfluentProducer(driver)
	result := make(chan error, 1)
	go func() {
		_, err := producer.ProduceWithMetadata("orders", []Message{{Value: []byte("one")}, {Value: []byte("two")}}, -1)
		result <- err
	}()

	<-firstReported
	select {
	case err := <-result:
		t.Fatalf("ProduceWithMetadata returned before all accepted reports were drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseSecond)
	<-secondReported
	err := <-result
	if !errors.Is(err, deliveryErr) {
		t.Fatalf("ProduceWithMetadata error = %v, want delivery error", err)
	}

	// librdkafka owns delivery channels. A send after the fit call returns must
	// not panic because fit never closes a channel supplied to the driver.
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("delivery channel was closed by fit: %v", recovered)
			}
		}()
		deliveryChan <- successfulDelivery(nil, 3)
	}()
}

func TestConfluentProducerDrainsAcceptedReportsAfterEnqueueFailure(t *testing.T) {
	enqueueErr := errors.New("queue full")
	secondAttempted := make(chan struct{})
	releaseFirst := make(chan struct{})

	produceCalls := 0
	driver := &fakeConfluentProducerDriver{}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		produceCalls++
		if produceCalls == 1 {
			go func() {
				<-releaseFirst
				reports <- successfulDelivery(message, 1)
			}()
			return nil
		}
		close(secondAttempted)
		return enqueueErr
	}

	producer := newTestConfluentProducer(driver)
	result := make(chan error, 1)
	go func() {
		result <- producer.Produce("orders", []Message{{Value: []byte("one")}, {Value: []byte("two")}}, -1)
	}()

	<-secondAttempted
	select {
	case err := <-result:
		t.Fatalf("Produce returned before the accepted report was drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	err := <-result
	if !errors.Is(err, enqueueErr) {
		t.Fatalf("Produce error = %v, want enqueue error", err)
	}
}

func TestConfluentProducerCloseWaitsForActiveDeliveryDrain(t *testing.T) {
	accepted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	closeCalled := make(chan struct{})

	driver := &fakeConfluentProducerDriver{closeCalled: closeCalled}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		close(accepted)
		go func() {
			<-releaseDelivery
			reports <- successfulDelivery(message, 1)
		}()
		return nil
	}
	producer := newTestConfluentProducer(driver)

	produceResult := make(chan error, 1)
	go func() {
		produceResult <- producer.Produce("orders", []Message{{Value: []byte("one")}}, -1)
	}()
	<-accepted

	closeResult := make(chan error, 1)
	go func() { closeResult <- producer.Close() }()
	select {
	case <-closeCalled:
		t.Fatal("driver closed before the active delivery report was drained")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseDelivery)
	if err := <-produceResult; err != nil {
		t.Fatalf("Produce error = %v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close error = %v", err)
	}
	<-closeCalled
}

func TestConfluentProducerKafkaJSDisconnectReturnsBeforeAcceptedDelivery(t *testing.T) {
	accepted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	driverClosed := make(chan struct{})
	driver := &fakeConfluentProducerDriver{closeCalled: driverClosed}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		close(accepted)
		go func() {
			<-releaseDelivery
			reports <- successfulDelivery(message, 1)
		}()
		return nil
	}
	producer := newTestConfluentProducer(driver)
	producer.closePolicy = ProducerCloseKafkaJSDisconnect

	produceResult := make(chan error, 1)
	go func() {
		produceResult <- producer.Produce("orders", []Message{{Value: []byte("one")}}, -1)
	}()
	<-accepted

	started := time.Now()
	if err := producer.Close(); err != nil {
		t.Fatalf("KafkaJS-compatible Close error = %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 250*time.Millisecond {
		t.Fatalf("KafkaJS-compatible Close took %s, want immediate return", elapsed)
	}
	select {
	case <-driverClosed:
		t.Fatal("driver closed before accepted delivery drained")
	default:
	}

	close(releaseDelivery)
	if err := <-produceResult; err != nil {
		t.Fatalf("accepted produce error = %v", err)
	}
	select {
	case <-driverClosed:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("driver was not closed after accepted delivery drained")
	}
}

func TestConfluentProducerKafkaJSAwaitDeliverySkipsUnresponsiveFlushWhenIdle(t *testing.T) {
	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFlush) }) }
	t.Cleanup(release)

	driver := &fakeConfluentProducerDriver{}
	driver.flushFn = func(int) int {
		close(flushStarted)
		<-releaseFlush
		return 0
	}
	producer := newTestConfluentProducer(driver)
	producer.closePolicy = ProducerCloseKafkaJSAwaitDelivery
	producer.closeTimeout = 50 * time.Millisecond

	started := time.Now()
	if err := producer.Close(); err != nil {
		t.Fatalf("awaited KafkaJS Close: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 250*time.Millisecond {
		t.Fatalf("awaited KafkaJS Close took %s with no pending reports", elapsed)
	}
	select {
	case <-flushStarted:
		t.Fatal("awaited KafkaJS Close called Flush with no pending reports")
	default:
	}
	flushCalls, closeCalls := driver.calls()
	if flushCalls != 0 || closeCalls != 1 {
		t.Fatalf("driver calls = flush %d close %d, want 0/1", flushCalls, closeCalls)
	}
	if err := producer.Produce("orders", []Message{{Value: []byte("late")}}, -1); err == nil || !strings.Contains(err.Error(), "producer is closed") {
		t.Fatalf("post-close Produce error = %v, want admission rejected", err)
	}
}

func TestConfluentProducerKafkaJSAwaitDeliveryDrainsBeforeConcurrentCloseReturns(t *testing.T) {
	accepted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	driverClosed := make(chan struct{})
	driver := &fakeConfluentProducerDriver{closeCalled: driverClosed}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		close(accepted)
		go func() {
			<-releaseDelivery
			reports <- successfulDelivery(message, 3)
		}()
		return nil
	}
	producer := newTestConfluentProducer(driver)
	producer.closePolicy = ProducerCloseKafkaJSAwaitDelivery
	producer.closeTimeout = time.Second

	produceResult := make(chan error, 1)
	go func() {
		produceResult <- producer.Produce("orders", []Message{{Value: []byte("one")}}, -1)
	}()
	<-accepted
	waitForConfluentPendingReports(t, producer, 1)

	firstClose := make(chan error, 1)
	secondClose := make(chan error, 1)
	go func() { firstClose <- producer.Close() }()
	go func() { secondClose <- producer.Close() }()
	select {
	case err := <-firstClose:
		t.Fatalf("first Close returned before delivery: %v", err)
	case err := <-secondClose:
		t.Fatalf("second Close returned before delivery: %v", err)
	case <-driverClosed:
		t.Fatal("driver closed before accepted delivery drained")
	case <-time.After(30 * time.Millisecond):
	}
	flushCalls, closeCalls := driver.calls()
	if flushCalls != 0 || closeCalls != 0 {
		t.Fatalf("pre-delivery driver calls = flush %d close %d, want 0/0", flushCalls, closeCalls)
	}

	close(releaseDelivery)
	if err := <-produceResult; err != nil {
		t.Fatalf("Produce: %v", err)
	}
	for name, result := range map[string]<-chan error{"first": firstClose, "second": secondClose} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s Close: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s Close did not finish after delivery", name)
		}
	}
	select {
	case <-driverClosed:
	case <-time.After(time.Second):
		t.Fatal("driver did not close after delivery drain")
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	flushCalls, closeCalls = driver.calls()
	if flushCalls != 0 || closeCalls != 1 {
		t.Fatalf("final driver calls = flush %d close %d, want 0/1", flushCalls, closeCalls)
	}
}

func TestConfluentProducerKafkaJSAwaitDeliveryTimeoutReportsPendingAndCleansUp(t *testing.T) {
	accepted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	driverClosed := make(chan struct{})
	driver := &fakeConfluentProducerDriver{closeCalled: driverClosed}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		close(accepted)
		go func() {
			<-releaseDelivery
			reports <- successfulDelivery(message, 5)
		}()
		return nil
	}
	producer := newTestConfluentProducer(driver)
	producer.closePolicy = ProducerCloseKafkaJSAwaitDelivery
	producer.closeTimeout = 35 * time.Millisecond

	produceResult := make(chan error, 1)
	go func() {
		produceResult <- producer.Produce("orders", []Message{{Value: []byte("one")}}, -1)
	}()
	<-accepted
	waitForConfluentPendingReports(t, producer, 1)

	started := time.Now()
	closeErr := producer.Close()
	if closeErr == nil || !strings.Contains(closeErr.Error(), "1 accepted delivery report(s) outstanding") {
		t.Fatalf("Close error = %v, want accurate pending-report timeout", closeErr)
	}
	if elapsed := time.Since(started); elapsed >= 250*time.Millisecond {
		t.Fatalf("Close took %s, want bounded return", elapsed)
	}
	if secondErr := producer.Close(); secondErr == nil || secondErr.Error() != closeErr.Error() {
		t.Fatalf("second Close = %v, want idempotent %v", secondErr, closeErr)
	}
	flushCalls, closeCalls := driver.calls()
	if flushCalls != 0 || closeCalls != 0 {
		t.Fatalf("timed-out driver calls = flush %d close %d, want 0/0", flushCalls, closeCalls)
	}
	select {
	case <-driverClosed:
		t.Fatal("driver closed while a delivery report was outstanding")
	default:
	}

	close(releaseDelivery)
	if err := <-produceResult; err != nil {
		t.Fatalf("Produce: %v", err)
	}
	select {
	case <-driverClosed:
	case <-time.After(time.Second):
		t.Fatal("background cleanup did not close the driver after delivery")
	}
	waitForConfluentPendingReports(t, producer, 0)
	if thirdErr := producer.Close(); thirdErr == nil || thirdErr.Error() != closeErr.Error() {
		t.Fatalf("post-cleanup Close = %v, want cached %v", thirdErr, closeErr)
	}
	flushCalls, closeCalls = driver.calls()
	if flushCalls != 0 || closeCalls != 1 {
		t.Fatalf("cleanup driver calls = flush %d close %d, want 0/1", flushCalls, closeCalls)
	}
}

func TestConfluentProducerClosePolicyDefaultsAndValidation(t *testing.T) {
	client, err := NewConfluentClient(&Config{Brokers: []string{"localhost:9092"}})
	if err != nil {
		t.Fatalf("NewConfluentClient: %v", err)
	}
	producer, err := client.Producer(ProducerConfig{})
	if err != nil {
		t.Fatalf("default producer: %v", err)
	}
	if got := producer.(*ConfluentProducer).closePolicy; got != ProducerCloseWaitForDelivery {
		t.Fatalf("default close policy = %d, want synchronous delivery drain", got)
	}
	if _, err = client.Producer(ProducerConfig{ClosePolicy: ProducerCloseKafkaJSAwaitDelivery}); err != nil {
		t.Fatalf("awaited KafkaJS close policy: %v", err)
	}
	_, err = client.Producer(ProducerConfig{ClosePolicy: ProducerClosePolicy(255)})
	if err == nil || err.Error() != "kafka/confluent: unsupported producer close policy 255" {
		t.Fatalf("unknown close policy error = %v", err)
	}
}

func TestConfluentProducerCloseReportsUndeliveredFlushCount(t *testing.T) {
	driver := &fakeConfluentProducerDriver{flushRemaining: 3}
	producer := newTestConfluentProducer(driver)

	err := producer.Close()
	if err == nil || !strings.Contains(err.Error(), "3 message(s) undelivered") {
		t.Fatalf("Close error = %v, want nonzero flush error", err)
	}
	if secondErr := producer.Close(); secondErr == nil || secondErr.Error() != err.Error() {
		t.Fatalf("second Close error = %v, want idempotent %v", secondErr, err)
	}
	flushCalls, closeCalls := driver.calls()
	if flushCalls != 1 || closeCalls != 1 {
		t.Fatalf("driver calls = flush %d close %d, want one each", flushCalls, closeCalls)
	}
}

func TestConfluentProducerProduceCtxCancellationKeepsDrainingAcceptedReports(t *testing.T) {
	accepted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	reported := make(chan struct{})
	closeCalled := make(chan struct{})
	driver := &fakeConfluentProducerDriver{closeCalled: closeCalled}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		close(accepted)
		go func() {
			<-releaseDelivery
			reports <- successfulDelivery(message, 9)
			close(reported)
		}()
		return nil
	}
	producer := newTestConfluentProducer(driver)
	producer.closeTimeout = 35 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- producer.ProduceCtx(ctx, "orders", []Message{{Value: []byte("one")}}, -1)
	}()
	<-accepted
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ProduceCtx error = %v, want context canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("ProduceCtx did not honor cancellation")
	}

	started := time.Now()
	closeErr := producer.Close()
	if closeErr == nil || !strings.Contains(closeErr.Error(), "timed out") {
		t.Fatalf("Close error = %v, want bounded timeout", closeErr)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("Close took %s, want bounded return", elapsed)
	}
	_, closeCalls := driver.calls()
	if closeCalls != 0 {
		t.Fatal("driver closed before the accepted delivery report was drained")
	}

	close(releaseDelivery)
	<-reported
	select {
	case <-closeCalled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("background shutdown did not close driver after delivery drain")
	}
	if pending := producer.pendingReports.Load(); pending != 0 {
		t.Fatalf("pending delivery reports = %d, want 0", pending)
	}
}

func TestConfluentProducerProduceBatchCtxHonorsCancellationAndDrains(t *testing.T) {
	accepted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	reported := make(chan struct{})
	driver := &fakeConfluentProducerDriver{}
	driver.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		close(accepted)
		go func() {
			<-releaseDelivery
			reports <- successfulDelivery(message, 3)
			close(reported)
		}()
		return nil
	}
	producer := newTestConfluentProducer(driver)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- producer.ProduceBatchCtx(ctx, []TopicMessages{{
			Topic: "orders", Messages: []Message{{Value: []byte("one")}},
		}}, -1)
	}()
	<-accepted
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ProduceBatchCtx error = %v, want context canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("ProduceBatchCtx did not honor cancellation")
	}

	close(releaseDelivery)
	<-reported
	if err := producer.Close(); err != nil {
		t.Fatalf("Close after drained batch error = %v", err)
	}
}

func TestConfluentProducerCloseIsBoundedWhenDriverFlushBlocks(t *testing.T) {
	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	closeCalled := make(chan struct{})
	driver := &fakeConfluentProducerDriver{closeCalled: closeCalled}
	driver.flushFn = func(int) int {
		close(flushStarted)
		<-releaseFlush
		return 0
	}
	producer := newTestConfluentProducer(driver)
	producer.closeTimeout = 30 * time.Millisecond

	started := time.Now()
	err := producer.Close()
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("Close error = %v, want timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("Close took %s with blocked Flush", elapsed)
	}
	secondStarted := time.Now()
	secondErr := producer.Close()
	if secondErr == nil || secondErr.Error() != err.Error() {
		t.Fatalf("second Close error = %v, want idempotent %v", secondErr, err)
	}
	if elapsed := time.Since(secondStarted); elapsed > 250*time.Millisecond {
		t.Fatalf("second Close took %s after bounded result", elapsed)
	}
	<-flushStarted
	select {
	case <-closeCalled:
		t.Fatal("driver Close raced a still-running Flush")
	default:
	}

	close(releaseFlush)
	select {
	case <-closeCalled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("driver was not closed after Flush completed")
	}
}

func TestConfluentProducerPreservesPerCallAcksCache(t *testing.T) {
	defaultDriver := &fakeConfluentProducerDriver{}
	producer := newTestConfluentProducer(defaultDriver)

	var mu sync.Mutex
	createdAcks := make([]any, 0, 2)
	createdDrivers := make([]*fakeConfluentProducerDriver, 0, 2)
	producer.newProducer = func(config *ckafka.ConfigMap) (confluentProducerDriver, error) {
		acks, err := config.Get("acks", "")
		if err != nil {
			return nil, err
		}
		driver := &fakeConfluentProducerDriver{}
		mu.Lock()
		createdAcks = append(createdAcks, acks)
		createdDrivers = append(createdDrivers, driver)
		mu.Unlock()
		return driver, nil
	}

	for _, acks := range []int{1, 1, 0, 0} {
		if err := producer.Produce("orders", []Message{{Value: []byte("value")}}, acks); err != nil {
			t.Fatalf("Produce(acks=%d) error = %v", acks, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(createdAcks) != 2 || fmt.Sprint(createdAcks[0]) != "1" || fmt.Sprint(createdAcks[1]) != "0" {
		t.Fatalf("created producer acks = %v, want one producer each for 1 and 0", createdAcks)
	}
	if producer.producers[1] != createdDrivers[0] || producer.producers[0] != createdDrivers[1] {
		t.Fatal("per-call acks producers were not cached under their acknowledgement level")
	}
}

type fakeConfluentConsumerDriver struct {
	readFn          func(time.Duration) (*ckafka.Message, error)
	commitFn        func(*ckafka.Message) ([]ckafka.TopicPartition, error)
	commitOffsetsFn func([]ckafka.TopicPartition) ([]ckafka.TopicPartition, error)
	storeFn         func(*ckafka.Message) ([]ckafka.TopicPartition, error)
	closeFn         func() error

	mu                sync.Mutex
	readTimeouts      []time.Duration
	commitCalls       int
	commitOffsetCalls [][]ckafka.TopicPartition
	storeCalls        int
	closeCalls        int
}

func (f *fakeConfluentConsumerDriver) ReadMessage(timeout time.Duration) (*ckafka.Message, error) {
	f.mu.Lock()
	f.readTimeouts = append(f.readTimeouts, timeout)
	readFn := f.readFn
	f.mu.Unlock()
	if readFn == nil {
		return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
	}
	return readFn(timeout)
}

func (f *fakeConfluentConsumerDriver) CommitMessage(message *ckafka.Message) ([]ckafka.TopicPartition, error) {
	f.mu.Lock()
	f.commitCalls++
	commitFn := f.commitFn
	f.mu.Unlock()
	if commitFn == nil {
		return nil, nil
	}
	return commitFn(message)
}

func (f *fakeConfluentConsumerDriver) CommitOffsets(offsets []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
	f.mu.Lock()
	cloned := append([]ckafka.TopicPartition(nil), offsets...)
	f.commitOffsetCalls = append(f.commitOffsetCalls, cloned)
	commitOffsetsFn := f.commitOffsetsFn
	f.mu.Unlock()
	if commitOffsetsFn == nil {
		return offsets, nil
	}
	return commitOffsetsFn(offsets)
}

func (f *fakeConfluentConsumerDriver) StoreMessage(message *ckafka.Message) ([]ckafka.TopicPartition, error) {
	f.mu.Lock()
	f.storeCalls++
	storeFn := f.storeFn
	f.mu.Unlock()
	if storeFn == nil {
		return nil, nil
	}
	return storeFn(message)
}

func (f *fakeConfluentConsumerDriver) Close() error {
	f.mu.Lock()
	f.closeCalls++
	closeFn := f.closeFn
	f.mu.Unlock()
	if closeFn == nil {
		return nil
	}
	return closeFn()
}

func (f *fakeConfluentConsumerDriver) reads() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.readTimeouts...)
}

func (f *fakeConfluentConsumerDriver) operationCalls() (commits, stores, closes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commitCalls, f.storeCalls, f.closeCalls
}

func (f *fakeConfluentConsumerDriver) exactOffsetCommits() [][]ckafka.TopicPartition {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([][]ckafka.TopicPartition, len(f.commitOffsetCalls))
	for i := range f.commitOffsetCalls {
		result[i] = append([]ckafka.TopicPartition(nil), f.commitOffsetCalls[i]...)
	}
	return result
}

func newTestConfluentConsumer(autoCommit bool, driver confluentConsumerDriver) *ConfluentConsumer {
	return &ConfluentConsumer{
		groupID: "test-group",
		config: ConsumerConfig{
			AutoCommit: autoCommit,
		},
		logger:   mustLogger(),
		consumer: driver,
	}
}

func boolPointer(value bool) *bool { return &value }

func TestConfluentConsumerRejectsInvalidRunOptionsBeforePoll(t *testing.T) {
	tests := []struct {
		name  string
		opts  ConsumerOptions
		want  string
		batch bool
	}{
		{
			name: "negative concurrency",
			opts: ConsumerOptions{PartitionsConsumedConcurrently: -1},
			want: "PartitionsConsumedConcurrently must not be negative",
		},
		{
			name:  "negative max records",
			opts:  ConsumerOptions{MaxRecords: -1},
			want:  "MaxRecords must not be negative",
			batch: true,
		},
		{
			name: "negative poll timeout",
			opts: ConsumerOptions{PollTimeout: -time.Millisecond},
			want: "PollTimeout must not be negative",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := &fakeConfluentConsumerDriver{}
			consumer := newTestConfluentConsumer(true, driver)
			var err error
			if test.batch {
				err = consumer.ConsumeBatch(func(BatchPayload) error { return nil }, test.opts)
			} else {
				err = consumer.Consume(func(MessagePayload) error { return nil }, test.opts)
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Consume error = %v, want %q", err, test.want)
			}
			if reads := driver.reads(); len(reads) != 0 {
				t.Fatalf("driver polled before options were rejected: %v", reads)
			}
		})
	}
}

func TestConfluentConsumerRunAutoCommitOverridesAreOperational(t *testing.T) {
	stopErr := errors.New("stop after one message")
	tests := []struct {
		name            string
		configured      bool
		override        *bool
		wantCommitCalls int
		wantStoreCalls  int
	}{
		{
			name:           "configured automatic uses post-handler offset store",
			configured:     true,
			wantStoreCalls: 1,
		},
		{
			name:            "manual override on automatic consumer commits synchronously",
			configured:      true,
			override:        boolPointer(false),
			wantCommitCalls: 1,
		},
		{
			name:            "automatic override on manual consumer resolves synchronously",
			configured:      false,
			override:        boolPointer(true),
			wantCommitCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			topic := "orders"
			reads := 0
			driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
				reads++
				if reads == 1 {
					return &ckafka.Message{TopicPartition: ckafka.TopicPartition{
						Topic: &topic, Partition: 0, Offset: 4,
					}}, nil
				}
				return nil, stopErr
			}}
			consumer := newTestConfluentConsumer(test.configured, driver)
			err := consumer.Consume(func(MessagePayload) error { return nil }, ConsumerOptions{AutoCommit: test.override})
			if !errors.Is(err, stopErr) {
				t.Fatalf("Consume error = %v, want stop error", err)
			}
			commits, stores, _ := driver.operationCalls()
			if commits != test.wantCommitCalls || stores != test.wantStoreCalls {
				t.Fatalf("offset operations = commits %d stores %d, want %d/%d", commits, stores, test.wantCommitCalls, test.wantStoreCalls)
			}
		})
	}
}

func TestConfluentConsumerCloseCancelsHandlerAndWaitsForCommit(t *testing.T) {
	topic := "orders"
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	releaseHandler := make(chan struct{})
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	driverClosed := make(chan struct{})

	reads := 0
	driver := &fakeConfluentConsumerDriver{
		readFn: func(time.Duration) (*ckafka.Message, error) {
			reads++
			if reads == 1 {
				return &ckafka.Message{TopicPartition: ckafka.TopicPartition{
					Topic: &topic, Partition: 1, Offset: 8,
				}}, nil
			}
			return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
		},
		commitFn: func(*ckafka.Message) ([]ckafka.TopicPartition, error) {
			close(commitStarted)
			<-releaseCommit
			return nil, nil
		},
		closeFn: func() error {
			close(driverClosed)
			return nil
		},
	}
	consumer := newTestConfluentConsumer(false, driver)
	consumeResult := make(chan error, 1)
	go func() {
		consumeResult <- consumer.ConsumeCtx(func(ctx context.Context, _ MessagePayload) error {
			close(handlerStarted)
			go func() {
				<-ctx.Done()
				close(handlerCanceled)
			}()
			<-releaseHandler
			return nil
		}, ConsumerOptions{})
	}()
	<-handlerStarted

	closeResult := make(chan error, 1)
	go func() { closeResult <- consumer.Close() }()
	select {
	case <-handlerCanceled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close did not cancel the active handler context")
	}
	select {
	case <-driverClosed:
		t.Fatal("driver closed while handler was active")
	default:
	}

	close(releaseHandler)
	select {
	case <-commitStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("successful handler did not reach offset commit")
	}
	select {
	case <-driverClosed:
		t.Fatal("driver closed while commit was active")
	default:
	}

	close(releaseCommit)
	if err := <-consumeResult; err != nil {
		t.Fatalf("ConsumeCtx error = %v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close error = %v", err)
	}
	<-driverClosed
}

func TestConfluentConsumerProcessesIndependentPartitionsConcurrently(t *testing.T) {
	topic := "orders"
	messages := []*ckafka.Message{
		{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1}},
		{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 2}},
	}
	readIndex := 0
	driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		if readIndex < len(messages) {
			message := messages[readIndex]
			readIndex++
			return message, nil
		}
		return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
	}}
	consumer := newTestConfluentConsumer(false, driver)
	started := make(chan int, 2)
	release := make(chan struct{})
	consumeResult := make(chan error, 1)
	go func() {
		consumeResult <- consumer.Consume(func(payload MessagePayload) error {
			started <- payload.Partition
			<-release
			return nil
		}, ConsumerOptions{PartitionsConsumedConcurrently: 2, MaxRecords: 2})
	}()

	seen := map[int]bool{}
	for len(seen) < 2 {
		select {
		case partition := <-started:
			seen[partition] = true
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("handlers did not run concurrently; started partitions %v", seen)
		}
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- consumer.Close() }()
	close(release)
	if err := <-consumeResult; err != nil {
		t.Fatalf("Consume error = %v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close error = %v", err)
	}
}

func TestConfluentConsumerKeepsSamePartitionOrderedWithConcurrency(t *testing.T) {
	topic := "orders"
	messages := []*ckafka.Message{
		{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1}},
		{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 2}},
	}
	readIndex := 0
	driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		if readIndex < len(messages) {
			message := messages[readIndex]
			readIndex++
			return message, nil
		}
		return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
	}}
	consumer := newTestConfluentConsumer(false, driver)
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	consumeResult := make(chan error, 1)
	go func() {
		consumeResult <- consumer.Consume(func(payload MessagePayload) error {
			if payload.Offset == 1 {
				close(firstStarted)
				<-releaseFirst
			} else {
				close(secondStarted)
			}
			return nil
		}, ConsumerOptions{PartitionsConsumedConcurrently: 2, MaxRecords: 2})
	}()
	<-firstStarted
	select {
	case <-secondStarted:
		t.Fatal("same-partition second record started before the first completed")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("same-partition second record did not run after the first")
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
	if err := <-consumeResult; err != nil {
		t.Fatalf("Consume error = %v", err)
	}
}

func TestConfluentConsumerUsesConfiguredPollTimeout(t *testing.T) {
	stopErr := errors.New("stop polling")
	tests := []struct {
		name    string
		timeout time.Duration
		batch   bool
	}{
		{name: "message", timeout: 237 * time.Millisecond},
		{name: "batch", timeout: 313 * time.Millisecond, batch: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
				return nil, stopErr
			}}
			consumer := newTestConfluentConsumer(true, driver)
			opts := ConsumerOptions{PollTimeout: test.timeout}
			var err error
			if test.batch {
				err = consumer.ConsumeBatch(func(BatchPayload) error { return nil }, opts)
			} else {
				err = consumer.Consume(func(MessagePayload) error { return nil }, opts)
			}
			if !errors.Is(err, stopErr) {
				t.Fatalf("Consume error = %v, want stop error", err)
			}
			reads := driver.reads()
			if len(reads) != 1 || reads[0] > test.timeout || test.timeout-reads[0] > time.Millisecond {
				t.Fatalf("poll timeouts = %v, want [%s]", reads, test.timeout)
			}
		})
	}
}

func TestConfluentConsumerSurfacesPostHandlerCommitFailures(t *testing.T) {
	commitErr := errors.New("commit rejected")
	tests := []struct {
		name  string
		batch bool
	}{
		{name: "message"},
		{name: "batch", batch: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			topic := "orders"
			readCalls := 0
			driver := &fakeConfluentConsumerDriver{
				readFn: func(time.Duration) (*ckafka.Message, error) {
					readCalls++
					if readCalls > 1 {
						return nil, fmt.Errorf("unexpected second poll")
					}
					return &ckafka.Message{TopicPartition: ckafka.TopicPartition{
						Topic:     &topic,
						Partition: 1,
						Offset:    7,
					}}, nil
				},
				commitFn: func(*ckafka.Message) ([]ckafka.TopicPartition, error) {
					return nil, commitErr
				},
			}
			consumer := newTestConfluentConsumer(false, driver)
			var err error
			if test.batch {
				err = consumer.ConsumeBatch(func(BatchPayload) error { return nil }, ConsumerOptions{MaxRecords: 1})
			} else {
				err = consumer.Consume(func(MessagePayload) error { return nil }, ConsumerOptions{})
			}
			if !errors.Is(err, commitErr) || !strings.Contains(err.Error(), "post-handler") {
				t.Fatalf("Consume error = %v, want surfaced post-handler commit error", err)
			}
		})
	}
}

func TestConfluentConsumerOffsetFinalizerCommitsExactCurrentOffsetAfterHandlerFailure(t *testing.T) {
	topic := "application-events"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{
		Topic:     &topic,
		Partition: 3,
		Offset:    41,
	}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	handlerErr := errors.New("handler failed")
	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)
	finalizerCalls := 0

	err := consumer.processMessageGroup(
		context.Background(),
		driver,
		groups[0],
		func(context.Context, MessagePayload) error { return handlerErr },
		false,
		ConsumerOptions{OffsetFinalizer: func(_ context.Context, payload MessagePayload, gotHandlerErr error, commit ExactOffsetCommit) error {
			finalizerCalls++
			if payload.Topic != topic || payload.Partition != 3 || payload.Offset != 41 {
				t.Fatalf("finalizer payload = %#v", payload)
			}
			if !errors.Is(gotHandlerErr, handlerErr) {
				t.Fatalf("finalizer handler error = %v, want %v", gotHandlerErr, handlerErr)
			}
			if err := commit(payload.Offset); err != nil {
				return err
			}
			return gotHandlerErr
		}},
	)
	if !errors.Is(err, handlerErr) {
		t.Fatalf("processMessageGroup error = %v, want handler error", err)
	}
	if finalizerCalls != 1 {
		t.Fatalf("finalizer calls = %d, want 1", finalizerCalls)
	}
	exactCommits := driver.exactOffsetCommits()
	if len(exactCommits) != 1 || len(exactCommits[0]) != 1 {
		t.Fatalf("exact commits = %#v, want one single-offset commit", exactCommits)
	}
	committed := exactCommits[0][0]
	if committed.Topic == nil || *committed.Topic != topic || committed.Partition != 3 || committed.Offset != 41 {
		t.Fatalf("committed offset = %#v, want %s[3]@41 exactly", committed, topic)
	}
	commits, stores, _ := driver.operationCalls()
	if commits != 0 || stores != 0 {
		t.Fatalf("standard offset operations = commits %d stores %d, want 0/0", commits, stores)
	}
}

func TestConfluentConsumerOffsetFinalizerOwnsPostCommitOrdering(t *testing.T) {
	topic := "application-events"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 12}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	var mu sync.Mutex
	events := make([]string, 0, 4)
	add := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}
	driver := &fakeConfluentConsumerDriver{commitOffsetsFn: func([]ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
		add("commit")
		return nil, nil
	}}
	consumer := newTestConfluentConsumer(false, driver)
	err := consumer.processMessageGroup(
		context.Background(), driver, groups[0],
		func(context.Context, MessagePayload) error {
			add("handler")
			return nil
		}, false,
		ConsumerOptions{OffsetFinalizer: func(_ context.Context, payload MessagePayload, handlerErr error, commit ExactOffsetCommit) error {
			add("finalizer")
			if handlerErr != nil {
				return handlerErr
			}
			if err := commit(payload.Offset); err != nil {
				return err
			}
			add("post-commit")
			return nil
		}},
	)
	if err != nil {
		t.Fatalf("processMessageGroup error = %v", err)
	}
	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	want := []string{"handler", "finalizer", "commit", "post-commit"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestConfluentConsumerOffsetFinalizerCanResolveAfterExactCommitOnSuccess(t *testing.T) {
	topic := "job-events"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 17}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	events := make([]string, 0, 4)
	driver := &fakeConfluentConsumerDriver{
		commitOffsetsFn: func(offsets []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
			events = append(events, fmt.Sprintf("exact:%d", offsets[0].Offset))
			return nil, nil
		},
		commitFn: func(message *ckafka.Message) ([]ckafka.TopicPartition, error) {
			events = append(events, fmt.Sprintf("resolved:%d", message.TopicPartition.Offset+1))
			return nil, nil
		},
	}
	consumer := newTestConfluentConsumer(false, driver)

	err := consumer.processMessageGroup(
		context.Background(), driver, groups[0],
		func(context.Context, MessagePayload) error {
			events = append(events, "handler")
			return nil
		}, false,
		ConsumerOptions{
			OffsetFinalizer: func(_ context.Context, payload MessagePayload, handlerErr error, commit ExactOffsetCommit) error {
				events = append(events, "finalizer")
				if handlerErr != nil {
					return handlerErr
				}
				return commit(payload.Offset)
			},
			ResolveAfterSuccessfulFinalizer: true,
		},
	)
	if err != nil {
		t.Fatalf("processMessageGroup error = %v", err)
	}
	want := []string{"handler", "finalizer", "exact:17", "resolved:18"}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestConfluentConsumerOffsetFinalizerCanClearKafkaJSCommitMetadata(t *testing.T) {
	topic := "job-events"
	memberID := "legacy-main-server-1234"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{
		Topic: &topic, Partition: 2, Offset: 17, Metadata: &memberID,
	}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	commits := make([]ckafka.TopicPartition, 0, 2)
	driver := &fakeConfluentConsumerDriver{
		commitOffsetsFn: func(offsets []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
			commits = append(commits, offsets[0])
			return offsets, nil
		},
		commitFn: func(*ckafka.Message) ([]ckafka.TopicPartition, error) {
			t.Fatal("KafkaJS null-metadata compatibility used CommitMessage")
			return nil, nil
		},
	}
	consumer := newTestConfluentConsumer(false, driver)

	err := consumer.processMessageGroup(
		context.Background(), driver, groups[0],
		func(context.Context, MessagePayload) error { return nil }, false,
		ConsumerOptions{
			OffsetFinalizer: func(_ context.Context, payload MessagePayload, handlerErr error, commit ExactOffsetCommit) error {
				if handlerErr != nil {
					return handlerErr
				}
				return commit(payload.Offset)
			},
			ResolveAfterSuccessfulFinalizer: true,
			NullOffsetCommitMetadata:        true,
		},
	)
	if err != nil {
		t.Fatalf("processMessageGroup error = %v", err)
	}
	if len(commits) != 2 {
		t.Fatalf("commit count = %d, want exact and resolved", len(commits))
	}
	for index, commit := range commits {
		if commit.Metadata != nil {
			t.Fatalf("commit[%d] metadata = %q, want nil", index, *commit.Metadata)
		}
	}
	if commits[0].Offset != 17 || commits[1].Offset != 18 {
		t.Fatalf("commit offsets = [%d %d], want [17 18]", commits[0].Offset, commits[1].Offset)
	}
}

func TestConfluentConsumerOffsetFinalizerDoesNotResolveSuppressedHandlerFailure(t *testing.T) {
	topic := "job-events"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 17}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	handlerErr := errors.New("dispatch failed")
	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)

	err := consumer.processMessageGroup(
		context.Background(), driver, groups[0],
		func(context.Context, MessagePayload) error { return handlerErr }, false,
		ConsumerOptions{
			OffsetFinalizer: func(_ context.Context, _ MessagePayload, got error, _ ExactOffsetCommit) error {
				if !errors.Is(got, handlerErr) {
					t.Fatalf("handler error = %v, want %v", got, handlerErr)
				}
				return nil
			},
			ResolveAfterSuccessfulFinalizer: true,
		},
	)
	if err != nil {
		t.Fatalf("processMessageGroup error = %v", err)
	}
	commits, stores, _ := driver.operationCalls()
	if commits != 0 || stores != 0 || len(driver.exactOffsetCommits()) != 0 {
		t.Fatalf("offset operations after suppressed failure = commits %d stores %d exact %#v, want none", commits, stores, driver.exactOffsetCommits())
	}
}

func TestConfluentConsumerOffsetFinalizerRejectsSecondCommit(t *testing.T) {
	topic := "application-events"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 12}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)
	err := consumer.processMessageGroup(
		context.Background(), driver, groups[0],
		func(context.Context, MessagePayload) error { return nil }, false,
		ConsumerOptions{OffsetFinalizer: func(_ context.Context, payload MessagePayload, _ error, commit ExactOffsetCommit) error {
			if err := commit(payload.Offset); err != nil {
				return err
			}
			return commit(payload.Offset)
		}},
	)
	if err == nil || !strings.Contains(err.Error(), "called more than once") {
		t.Fatalf("processMessageGroup error = %v", err)
	}
	if commits := driver.exactOffsetCommits(); len(commits) != 1 {
		t.Fatalf("exact commits = %#v, want only first commit", commits)
	}
}

func TestConfluentConsumerOffsetFinalizerCanSkipCommit(t *testing.T) {
	topic := "application-events"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 9}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	handlerErr := errors.New("before commit boundary")
	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)

	err := consumer.processMessageGroup(
		context.Background(), driver, groups[0],
		func(context.Context, MessagePayload) error { return handlerErr },
		false,
		ConsumerOptions{OffsetFinalizer: func(_ context.Context, _ MessagePayload, handlerErr error, _ ExactOffsetCommit) error {
			return handlerErr
		}},
	)
	if !errors.Is(err, handlerErr) {
		t.Fatalf("processMessageGroup error = %v, want handler error", err)
	}
	if commits := driver.exactOffsetCommits(); len(commits) != 0 {
		t.Fatalf("exact commits = %#v, want none", commits)
	}
}

func TestConfluentConsumerOffsetFinalizerFailurePrecedence(t *testing.T) {
	topic := "application-events"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 9}}
	groups := groupConfluentBatchMessages([]*ckafka.Message{message})
	handlerErr := errors.New("handler failed")
	finalizerErr := errors.New("finalizer failed")
	commitErr := errors.New("commit failed")

	t.Run("finalizer replaces handler", func(t *testing.T) {
		driver := &fakeConfluentConsumerDriver{}
		consumer := newTestConfluentConsumer(false, driver)
		err := consumer.processMessageGroup(
			context.Background(), driver, groups[0],
			func(context.Context, MessagePayload) error { return handlerErr }, false,
			ConsumerOptions{OffsetFinalizer: func(context.Context, MessagePayload, error, ExactOffsetCommit) error {
				return finalizerErr
			}},
		)
		if !errors.Is(err, finalizerErr) || errors.Is(err, handlerErr) {
			t.Fatalf("error = %v, want only finalizer error", err)
		}
		if commits := driver.exactOffsetCommits(); len(commits) != 0 {
			t.Fatalf("exact commits = %#v, want none", commits)
		}
	})

	t.Run("commit replaces handler", func(t *testing.T) {
		driver := &fakeConfluentConsumerDriver{commitOffsetsFn: func([]ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
			return nil, commitErr
		}}
		consumer := newTestConfluentConsumer(false, driver)
		err := consumer.processMessageGroup(
			context.Background(), driver, groups[0],
			func(context.Context, MessagePayload) error { return handlerErr }, false,
			ConsumerOptions{OffsetFinalizer: func(_ context.Context, payload MessagePayload, _ error, commit ExactOffsetCommit) error {
				return commit(payload.Offset)
			}},
		)
		if !errors.Is(err, commitErr) || errors.Is(err, handlerErr) {
			t.Fatalf("error = %v, want only commit error", err)
		}
	})
}

func TestConfluentConsumerOffsetFinalizerOptionValidation(t *testing.T) {
	finalizer := func(context.Context, MessagePayload, error, ExactOffsetCommit) error {
		return nil
	}
	autoCommit := true
	if _, _, _, err := validateConfluentConsumerOptions(false, ConsumerOptions{
		AutoCommit: &autoCommit, OffsetFinalizer: finalizer,
	}, time.Millisecond); err == nil || !strings.Contains(err.Error(), "requires manual commit") {
		t.Fatalf("auto-commit validation error = %v", err)
	}
	manualCommit := false
	if _, _, _, err := validateConfluentConsumerOptions(false, ConsumerOptions{
		AutoCommit: &manualCommit, CommitBeforeHandler: true, OffsetFinalizer: finalizer,
	}, time.Millisecond); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("commit-before validation error = %v", err)
	}
	consumer := newTestConfluentConsumer(false, &fakeConfluentConsumerDriver{})
	if err := consumer.ConsumeBatch(func(BatchPayload) error { return nil }, ConsumerOptions{OffsetFinalizer: finalizer}); err == nil || !strings.Contains(err.Error(), "message consumption") {
		t.Fatalf("batch validation error = %v", err)
	}
	if _, _, _, err := validateConfluentConsumerOptions(false, ConsumerOptions{
		ResolveAfterSuccessfulFinalizer: true,
	}, time.Millisecond); err == nil || !strings.Contains(err.Error(), "requires OffsetFinalizer") {
		t.Fatalf("resolve-after validation error = %v", err)
	}
	if _, _, _, err := validateConfluentConsumerOptions(false, ConsumerOptions{
		NullOffsetCommitMetadata: true,
	}, time.Millisecond); err != nil {
		t.Fatalf("manual null-metadata validation error = %v", err)
	}
	if _, _, _, err := validateConfluentConsumerOptions(true, ConsumerOptions{
		NullOffsetCommitMetadata: true,
	}, time.Millisecond); err == nil || !strings.Contains(err.Error(), "requires manual commit") {
		t.Fatalf("auto-commit null-metadata validation error = %v", err)
	}
	if _, _, _, err := validateConfluentConsumerOptions(false, ConsumerOptions{
		NullOffsetCommitMetadata: true, CommitBeforeHandler: true,
	}, time.Millisecond); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("commit-before null-metadata validation error = %v", err)
	}
}

func TestConfluentConsumerStopsPartitionAfterHandlerFailure(t *testing.T) {
	handlerErr := errors.New("handler rejected message")
	tests := []struct {
		name       string
		autoCommit bool
	}{
		{name: "manual commit"},
		{name: "automatic offset store", autoCommit: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			topic := "orders"
			messages := []*ckafka.Message{
				{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 7}},
				{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 8}},
			}
			groups := groupConfluentBatchMessages(messages)
			if len(groups) != 1 {
				t.Fatalf("partition groups = %d, want 1", len(groups))
			}

			driver := &fakeConfluentConsumerDriver{}
			consumer := newTestConfluentConsumer(test.autoCommit, driver)
			handledOffsets := make([]int64, 0, 1)
			err := consumer.processMessageGroup(
				context.Background(),
				driver,
				groups[0],
				func(_ context.Context, payload MessagePayload) error {
					handledOffsets = append(handledOffsets, payload.Offset)
					return handlerErr
				},
				test.autoCommit,
				ConsumerOptions{},
			)

			if !errors.Is(err, handlerErr) {
				t.Fatalf("processMessageGroup error = %v, want handler error", err)
			}
			if len(handledOffsets) != 1 || handledOffsets[0] != 7 {
				t.Fatalf("handled offsets = %v, want only failed offset 7", handledOffsets)
			}
			commits, stores, _ := driver.operationCalls()
			if commits != 0 || stores != 0 {
				t.Fatalf("offset operations after handler failure = commits %d stores %d, want 0/0", commits, stores)
			}
		})
	}
}
