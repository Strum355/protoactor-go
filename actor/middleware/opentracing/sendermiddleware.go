package opentracing

import (
	"fmt"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/opentracing/opentracing-go"
)

func SenderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			/* span := getActiveSpan(c.Self())

			if span == nil {
				logger.Debug("OUTBOUND No active span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				next(c, target, envelope)
				return
			} */

			spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapReader(&MessageHeaderReader{envelope.Header}))
			if err != nil {
				logger.Debug("OUTBOUND No active span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				next(c, target, envelope)
				return
			}

			carrier := opentracing.TextMapWriter(&MessageEnvelopeWriter{MessageEnvelope: envelope})
			err = opentracing.GlobalTracer().Inject(spanContext, opentracing.TextMap, carrier)
			if err != nil {
				logger.Debug("OUTBOUND Error injecting", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				next(c, target, envelope)
				return
			}

			keyMap := make(map[string]string)
			for _, key := range envelope.Header.Keys() {
				keyMap[key] = envelope.GetHeader(key)
			}
			logger.Debug(fmt.Sprintf("OUTBOUND HEADER KEY %s MESSAGE TYPE %T", keyMap, envelope.Message))

			logger.Debug("OUTBOUND Successfully injected", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
			next(c, target, envelope)
		}
	}
}
