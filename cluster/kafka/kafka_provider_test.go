package kafka

import (
	"fmt"
	"testing"

	"github.com/AsynkronIT/protoactor-go/cluster"
	"github.com/Shopify/sarama"
)

type testReporter struct{}

func (t *testReporter) Errorf(format string, v ...interface{}) {
	fmt.Errorf(format, v...)
}

/* func Test_RegisterMember(t *testing.T) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Errors = true

	consumer := mocks.NewConsumer(&testReporter{}, conf)
	expectPConsumer := consumer.ExpectConsumePartition(topic, 0, 1)
	expectPConsumer.ExpectMessagesDrainedOnClose()

	pConsumer, err := consumer.ConsumePartition(topic, 0, 0)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	producer := mocks.NewSyncProducer(&testReporter{}, conf)
	producer.ExpectSendMessageAndSucceed()

	provider := &KafkaProvider{
		consumer: pConsumer,
		produder: producer,
		shutdown: make(chan interface{}),
		consumeErrFunc: func(err error) {
			log.Println("[CLUSTER] [KAFKA] Error consuming Kafka message:", err)
		},
		consumerPreFunc: func(msg *sarama.ConsumerMessage) {
			t.Logf("got message: %v", msg)
		},
	}
	defer provider.Shutdown()
	provider.MonitorMemberStatusChanges()

	err = provider.RegisterMember("mycluster", "127.0.0.1", 8000, []string{"a", "b"}, nil, &cluster.NilMemberStatusValueSerializer{})
	if err != nil {
		t.Errorf("failed to register member: %v", err)
	}

	time.Sleep(time.Second * 5)
} */

func Test_RegisterMember(t *testing.T) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true

	provider := NewWithConfig([]string{"127.0.0.1:9092"}, ProviderConfig{Kafka: conf, Offset: sarama.OffsetNewest})
	provider.consumeErrFunc = func(err error) {
		t.Errorf("error consuming kafka message: %v", err)
	}

	defer func() {
		err := provider.Shutdown()
		if err != nil {
			t.Errorf("error shutting down: %v", err)
		}
	}()

	provider.MonitorMemberStatusChanges()

	err := provider.RegisterMember("mycluster", "127.0.0.1", 8000, []string{"a", "b"}, nil, &cluster.NilMemberStatusValueSerializer{})
	if err != nil {
		t.Errorf("failed to register member: %v", err)
	}
}
