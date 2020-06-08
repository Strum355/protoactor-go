package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/AsynkronIT/protoactor-go/cluster"
	"github.com/Shopify/sarama"
)

const (
	heartbeat = "HEARTBEAT"
	leaving   = "LEAVING"
	update    = "UPDATE"

	topic = "protoactor_control_plane"
)

type event struct {
	Source string                 `json:"source"`
	Type   string                 `json:"type"`
	Meta   map[string]interface{} `json:"meta"`
}

type KafkaProvider struct {
	id                    string
	shutdown              bool
	errorChan             chan error
	statusValue           cluster.MemberStatusValue
	statusValueSerializer cluster.MemberStatusValueSerializer
	consumer              sarama.PartitionConsumer
	producer              sarama.SyncProducer
	consumeErrFunc        func(error)
}

type ProviderConfig struct {
	// Kafka contains the configration for the Consumer and
	// Producer
	Kafka *sarama.Config
	// Offset sets the offsets within the topic partition to
	// start reading from
	Offset int64
	// Heartbeat sets the duration interval between every heartbeat
	// sent to the control plane topic. This also dictates the rate
	// at which the cluster member list is updated.
	Heartbeat time.Duration
}

func New(brokerAddrs []string) *KafkaProvider {
	return NewWithConfig(brokerAddrs, ProviderConfig{
		Kafka:  sarama.NewConfig(),
		Offset: sarama.OffsetNewest,
	})
}

func NewWithConfig(brokerAddrs []string, provConfig ProviderConfig) *KafkaProvider {
	consumer, err := sarama.NewConsumer(brokerAddrs, provConfig.Kafka)
	if err != nil {
		panic(fmt.Sprintf("failed to create kafka consumer: %v", err))
	}

	pConsumer, err := consumer.ConsumePartition(topic, 0, provConfig.Offset)
	if err != nil {
		panic(fmt.Sprintf("failed to create partition consumer: %v", err))
	}

	producer, err := sarama.NewSyncProducer(brokerAddrs, provConfig.Kafka)
	if err != nil {
		panic(fmt.Sprintf("failed to create kafka producer: %v", err))
	}

	return &KafkaProvider{
		consumer:  pConsumer,
		producer:  producer,
		errorChan: make(chan error),
		consumeErrFunc: func(err error) {
			log.Println("[CLUSTER] [KAFKA] Error consuming Kafka message:", err)
		},
	}
}

func (k *KafkaProvider) OnConsumeError(f func(error)) *KafkaProvider {
	k.consumeErrFunc = f
	return k
}

func (p *KafkaProvider) RegisterMember(clusterName string, address string, port int, knownKinds []string,
	statusValue cluster.MemberStatusValue, serializer cluster.MemberStatusValueSerializer) error {

	p.id = fmt.Sprintf("%s@%s:%d", clusterName, address, port)
	p.statusValue = statusValue
	p.statusValueSerializer = serializer

	b, err := json.Marshal(event{Source: p.id, Type: heartbeat, Meta: map[string]interface{}{
		"memberStatus": p.statusValueSerializer.ToValueBytes(statusValue),
	}})
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
	_, _, err = p.producer.SendMessage(msg)
	return err
}

func (p *KafkaProvider) MonitorMemberStatusChanges() {
	go func() {
		for !p.shutdown {
			select {
			case msg := <-p.consumer.Messages():
				p.notifyStatuses(msg)
			case err := <-p.consumer.Errors():
				p.consumeErrFunc(err)
			}
		}
	}()
}

func (p *KafkaProvider) collectUntil() {

}

func (p *KafkaProvider) notifyStatuses(msg *sarama.ConsumerMessage) {

}

// sends a heartbeat message to the kafka topic.
func (p *KafkaProvider) blockingHeartbeat() {

}

// starts the heartbeat in a
func (p *KafkaProvider) jumpstartHeart() {

}

func (p *KafkaProvider) UpdateMemberStatusValue(statusValue cluster.MemberStatusValue) error {
	b, err := json.Marshal(event{Source: p.id, Type: update, Meta: map[string]interface{}{
		"memberStatus": p.statusValueSerializer.ToValueBytes(statusValue),
	}})
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
	_, _, err = p.producer.SendMessage(msg)
	return err
}

func (p *KafkaProvider) DeregisterMember() error {
	b, err := json.Marshal(event{Source: p.id, Type: leaving})
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}
	_, _, err = p.producer.SendMessage(msg)
	return err
}

type shutdownErrors []error

func (s *shutdownErrors) Error() string {
	b := strings.Builder{}
	for _, err := range *s {
		b.WriteString(fmt.Sprintf("%v", err))
	}
	return b.String()
}

func (p *KafkaProvider) Shutdown() error {
	var err shutdownErrors

	deregErr := p.DeregisterMember()
	if err != nil {
		err = append(err, deregErr)
	}

	p.shutdown = true
	consumeErr := p.consumer.Close()
	p.producer.Close()

	if consumeErr != nil {
		err = append(err, consumeErr)
	}

	if len(err) > 0 {
		return &err
	}
	return nil
}
