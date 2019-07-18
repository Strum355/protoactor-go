package opentracing

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/opentracing/opentracing-go"
)

func SenderMiddleware() actor.SenderMiddleware {
	return func(next actor.SenderFunc) actor.SenderFunc {
		return func(c actor.SenderContext, target *actor.PID, envelope *actor.MessageEnvelope) {
			span := c.Span()
			if span == nil {
				logger.Debug("OUTBOUND No active span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				next(c, target, envelope)
				return
			}

			carrier := opentracing.TextMapWriter(&actor.MessageEnvelopeWriter{MessageEnvelope: envelope})
			err := opentracing.GlobalTracer().Inject(c.Span().Context(), opentracing.TextMap, carrier)
			if err != nil {
				logger.Debug("OUTBOUND Error injecting", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				next(c, target, envelope)
				return
			}

			logger.Debug("OUTBOUND Successfully injected", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
			next(c, target, envelope)
		}
	}
}
