package opentracing

import (
	"fmt"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/opentracing/opentracing-go"
)

func ReceiverMiddleware() actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {
			// after we have responded, we want to clear the current span
			defer c.ClearSpan()

			spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapReader(&actor.MessageHeaderReader{ReadOnlyMessageHeader: envelope.Header}))
			if err == opentracing.ErrSpanContextNotFound {
				logger.Debug(fmt.Sprintf("INBOUND No spanContext found for request type %T", envelope.Message), log.Stringer("PID", c.Self()), log.Error(err))
				next(c, envelope)
				return
			} else if err != nil {
				logger.Debug("INBOUND Error", log.Stringer("PID", c.Self()), log.Error(err))
				next(c, envelope)
				return
			}

			var span opentracing.Span
			switch envelope.Message.(type) {
			case *actor.Started:
				span = opentracing.StartSpan(fmt.Sprintf("%T/%T", c.Actor(), envelope.Message), opentracing.ChildOf(spanContext))
				logger.Debug("INBOUND Found parent span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
			case *actor.Stopping:
				span = opentracing.StartSpan(fmt.Sprintf("%T/stopping", c.Actor()), opentracing.ChildOf(spanContext))
				span.SetTag("actorPID", c.Self())
				span.SetTag("actorType", fmt.Sprintf("%T", c.Actor()))
				span.SetTag("messageType", fmt.Sprintf("%T", envelope.Message))
				defer span.Finish()

				next(c, envelope)
				return
			case *actor.Stopped:
				span = opentracing.StartSpan(fmt.Sprintf("%T/stopped", c.Actor()), opentracing.ChildOf(spanContext))
				defer span.Finish()
				next(c, envelope)
				return
			}

			if span == nil && spanContext == nil {
				logger.Debug("INBOUND No spanContext. Starting new span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				span = opentracing.StartSpan(fmt.Sprintf("%T/%T", c.Actor(), envelope.Message))
			}
			if span == nil {
				logger.Debug("INBOUND Starting span from parent", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				span = opentracing.StartSpan(fmt.Sprintf("%T/%T", c.Actor(), envelope.Message), opentracing.ChildOf(spanContext))
			}

			span.SetTag("actorPID", c.Self())
			span.SetTag("actorType", fmt.Sprintf("%T", c.Actor()))
			span.SetTag("messageType", fmt.Sprintf("%T", envelope.Message))

			c.SetSpan(span)

			next(c, envelope)

		}
	}
}
