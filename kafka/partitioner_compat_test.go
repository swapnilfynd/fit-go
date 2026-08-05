// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

type metadataConfluentProducerDriver struct {
	*fakeConfluentProducerDriver
	metadataFn func(*string, bool, int) (*ckafka.Metadata, error)
}

func (f *metadataConfluentProducerDriver) GetMetadata(
	topic *string,
	allTopics bool,
	timeoutMs int,
) (*ckafka.Metadata, error) {
	return f.metadataFn(topic, allTopics, timeoutMs)
}

func TestAutomaticMessageConstructorsPreservePartitionIntent(t *testing.T) {
	value := []byte("payload")
	key := []byte("entity-42")

	require.Equal(t, Message{Value: value, Partition: -1}, NewMessage(value))
	require.Equal(t, Message{Key: key, Value: value, Partition: -1}, NewKeyedMessage(key, value))
	require.Equal(t, Message{Value: value, Partition: 0}, NewPartitionedMessage(0, value))
	// Backward compatibility: old callers using a literal still explicitly
	// select partition zero. Only the new constructors opt into auto selection.
	require.Equal(t, int32(0), buildConfluentMessage("events", Message{Value: value}).TopicPartition.Partition)
	require.Equal(t, ckafka.PartitionAny, buildConfluentMessage("events", NewMessage(value)).TopicPartition.Partition)
}

func TestProducerConfigKafkaJSLegacyPartitionerIsExplicit(t *testing.T) {
	client, err := NewConfluentClient(&Config{Brokers: []string{"broker:9092"}})
	require.NoError(t, err)

	producerAPI, err := client.Producer(ProducerConfig{Partitioner: ProducerPartitionerKafkaJSLegacy})
	require.NoError(t, err)
	producer := producerAPI.(*ConfluentProducer)
	partitioner, err := producer.configMap.Get("partitioner", "")
	require.NoError(t, err)
	require.Equal(t, "murmur2_random", partitioner)
	require.Equal(t, ProducerPartitionerKafkaJSLegacy, producer.partitioner)

	defaultProducerAPI, err := client.Producer(ProducerConfig{})
	require.NoError(t, err)
	defaultProducer := defaultProducerAPI.(*ConfluentProducer)
	partitioner, err = defaultProducer.configMap.Get("partitioner", nil)
	require.NoError(t, err)
	require.Nil(t, partitioner)

	_, err = client.Producer(ProducerConfig{Partitioner: ProducerPartitioner("unknown")})
	require.EqualError(t, err, `kafka/confluent: unsupported producer partitioner "unknown"`)
}

func TestKafkaJSLegacyKeylessPartitionerMatchesRoundRobin(t *testing.T) {
	base := &fakeConfluentProducerDriver{}
	var producedPartitions []int32
	base.produceFn = func(message *ckafka.Message, reports chan ckafka.Event) error {
		producedPartitions = append(producedPartitions, message.TopicPartition.Partition)
		reports <- successfulDelivery(message, int64(len(producedPartitions)))
		return nil
	}
	driver := &metadataConfluentProducerDriver{
		fakeConfluentProducerDriver: base,
		metadataFn: func(topic *string, allTopics bool, timeoutMs int) (*ckafka.Metadata, error) {
			require.NotNil(t, topic)
			require.Equal(t, "events", *topic)
			require.False(t, allTopics)
			require.Equal(t, int(defaultProducerMetadataTimeout.Milliseconds()), timeoutMs)
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
	producer.partitioner = ProducerPartitionerKafkaJSLegacy
	producer.partitionSeed = func() (uint32, error) { return 0, nil }

	err := producer.Produce("events", []Message{
		NewMessage([]byte("first")),
		NewMessage([]byte("second")),
		NewMessage([]byte("third")),
	}, -1)
	require.NoError(t, err)
	require.Equal(t, []int32{2, 1, 2}, producedPartitions)
}

func TestKafkaJSLegacyPartitionerKeepsExplicitAndKeyedPaths(t *testing.T) {
	keyed := NewKeyedMessage([]byte{}, []byte("keyed"))
	keyedMessage := buildConfluentMessage("events", keyed)
	require.NotNil(t, keyedMessage.Key)
	require.Empty(t, keyedMessage.Key)
	require.Equal(t, ckafka.PartitionAny, keyedMessage.TopicPartition.Partition)

	explicit := NewPartitionedMessage(0, []byte("explicit"))
	require.Equal(t, int32(0), buildConfluentMessage("events", explicit).TopicPartition.Partition)
}

func TestKafkaJSLegacyNoLeaderFallbackMatchesKafkaJSArrayIndex(t *testing.T) {
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
	require.NoError(t, err)
	second, err := producer.nextKafkaJSKeylessPartition("events", metadata)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1}, []int32{first, second})
}

func TestKafkaJSLegacyKeyedPartitionMatchesKafkaJSFixture(t *testing.T) {
	cluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	require.NoError(t, cluster.CreateTopic("entity-events", 7, 1))

	client, err := NewConfluentClient(&Config{
		Brokers:  []string{cluster.BootstrapServers()},
		ClientID: "kafkajs-partitioner-fixture",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	producerAPI, err := client.Producer(ProducerConfig{Partitioner: ProducerPartitionerKafkaJSLegacy})
	require.NoError(t, err)
	producer := producerAPI.(*ConfluentProducer)
	require.NoError(t, producer.Connect())
	t.Cleanup(func() { require.NoError(t, producer.Close()) })

	metadata, err := producer.ProduceWithMetadata("entity-events", []Message{
		NewKeyedMessage([]byte("66aa11bb22cc33dd44ee5501"), []byte(`{"fixture":"entity"}`)),
	}, -1)
	require.NoError(t, err)
	require.Len(t, metadata, 1)
	require.Equal(t, 6, metadata[0].Partition, "must match KafkaJS 2.2.4 on a seven-partition topic")
}
