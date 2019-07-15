package remote

import (
	"github.com/opentracing/opentracing-go"
	"github.com/AsynkronIT/protoactor-go/actor"
)

func ReceiverMiddleware() actor.ReceiverMiddleware {
	return func(next actor.ReceiverFunc) actor.ReceiverFunc {
		return func(c actor.ReceiverContext, envelope *actor.MessageEnvelope) {
			spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, envelope)
			if err  == opentracing.ErrSpanContextNotFound {
				logger.Error("REMOTE INBOUND no spanContext found")
			}

			
		}
	}
}