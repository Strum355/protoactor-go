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
			logger.Debug(fmt.Sprintf("INBOUND HEADER KEYS %v MESSAGE TYPE %s\n", envelope.Header.ToMap(), fmt.Sprintf("%T", envelope.Message)))
			spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.TextMapReader(&MessageHeaderReader{ReadOnlyMessageHeader: envelope.Header}))
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
				span.SetTag("ActorPID", c.Self())
				span.SetTag("ActorType", fmt.Sprintf("%T", c.Actor()))
				span.SetTag("MessageType", fmt.Sprintf("%T", envelope.Message))
				span.SetTag("RequestID", envelope.GetHeader("RequestID"))
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

			span.SetTag("ActorPID", c.Self())
			span.SetTag("ActorType", fmt.Sprintf("%T", c.Actor()))
			span.SetTag("MessageType", fmt.Sprintf("%T", envelope.Message))
			span.SetTag("RequestID", envelope.GetHeader("RequestID"))
			fmt.Printf("SETTING SPAN TAGS %v FROM %v\n", envelope.GetHeader("RequestID"), fmt.Sprintf("%T", envelope.Message))
			defer func() {
				logger.Debug("INBOUND Finishing span", log.Stringer("PID", c.Self()), log.TypeOf("ActorType", c.Actor()), log.TypeOf("MessageType", envelope.Message))
				span.Finish()
			}()

			next(c, envelope)
		}
	}
}
