package opentelemetry

import (
	"github.com/ThreeDotsLabs/watermill/message"

	"go.opentelemetry.io/otel/trace"
)

// ExtractRemoteParentSpanContext defines a middleware that will extract trace/span id
// from the message metadata and creates a child span for the message.
func ExtractRemoteParentSpanContext() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return ExtractRemoteParentSpanContextHandler(h)
	}
}

// ExtractRemoteParentSpanContextHandler decorates a watermill HandlerFunc to extract
// trace/span id from the metadata when a message is received and set a child span context.
func ExtractRemoteParentSpanContextHandler(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {

		if msg.Metadata.Get("trace_id") != "" {

			var traceID trace.TraceID
			var spanID trace.SpanID
			var err error

			traceID, err = trace.TraceIDFromHex(msg.Metadata.Get("trace_id"))
			if err != nil {
				return nil, err
			}

			if msg.Metadata.Get("span_id") != "" {
				spanID, err = trace.SpanIDFromHex(msg.Metadata.Get("span_id"))
				if err != nil {
					return nil, err
				}
			}

			spanContext := trace.NewSpanContext(
				trace.SpanContextConfig{
					TraceID:    traceID,
					SpanID:     spanID,
					TraceFlags: 01,
					Remote:     true,
				},
			)

			if spanContext.IsValid() {
				msg.SetContext(trace.ContextWithSpanContext(msg.Context(), spanContext))
			}
		}

		return h(msg)
	}
}
