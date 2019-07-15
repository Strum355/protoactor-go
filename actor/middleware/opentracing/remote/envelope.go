package remote

import (
	_"fmt"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/opentracing/opentracing-go"
)

type messageHeaderReader struct {
	ReadOnlyMessageHeader actor.ReadonlyMessageHeader
}

func (reader *messageHeaderReader) ForeachKey(handler func(key, val string) error) error {
	if reader.ReadOnlyMessageHeader == nil {
		//fmt.Println("MESSAGE HEADER EMPTY")
		return nil
	}
	//fmt.Printf("KEY LENGTH %d\n", reader.ReadOnlyMessageHeader.Length())
	for _, key := range reader.ReadOnlyMessageHeader.Keys() {
		//fmt.Printf("READ KEY %s VALUE %s\n", key, reader.ReadOnlyMessageHeader.Get(key))
		err := handler(key, reader.ReadOnlyMessageHeader.Get(key))
		if err != nil {
			return err
		}
	}
	return nil
}

var _ opentracing.TextMapReader = &messageHeaderReader{}

type messageEnvelopeWriter struct {
	MessageEnvelope *actor.MessageEnvelope
}

func (writer *messageEnvelopeWriter) Set(key, val string) {
	//fmt.Printf("WRITE KEY %s VALUE %s\n", key, val)
	writer.MessageEnvelope.SetHeader(key, val)
	//writer.MessageEnvelope.SetHeader("asdf", "abcd")
}

var _ opentracing.TextMapWriter = &messageEnvelopeWriter{}
