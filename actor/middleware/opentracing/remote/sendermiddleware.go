package remote

import (
	_"fmt"
	"github.com/AsynkronIT/protoactor-go/actor"
	_"github.com/AsynkronIT/protoactor-go/log"
	"github.com/opentracing/opentracing-go"
)

func SenderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			carrier := opentracing.TextMapWriter(&messageEnvelopeWriter{MessageEnvelope: envelope})
			err := opentracing.GlobalTracer().Inject(c.Span(), opentracing.TextMap, carrier)
			if err != nil {

			}
			next(c, target, envelope)
		}
	}
}
