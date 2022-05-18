package opentelemetry

import (
	"github.com/ThreeDotsLabs/watermill/message"

	"go.opentelemetry.io/otel/trace"
)

// Decorator that adds trace/span id to the message metadata, so the subscriber(s) can extract this.
type TracePropagatingPublisherDecorator struct {
	pub message.Publisher
}

func NewTracePropagatingPublisherDecorator(pub message.Publisher) message.Publisher {
	return &TracePropagatingPublisherDecorator{pub}
}

func (p *TracePropagatingPublisherDecorator) Publish(topic string, messages ...*message.Message) error {
	if len(messages) == 0 {
		return nil
	}

	spanContext := trace.SpanFromContext(messages[0].Context()).SpanContext()

	for _, msg := range messages {
		if spanContext.HasTraceID() {
			msg.Metadata.Set("trace_id", spanContext.TraceID().String())
		}
		if spanContext.HasSpanID() {
			msg.Metadata.Set("span_id", spanContext.SpanID().String())
		}
	}

	return p.pub.Publish(topic, messages...)
}

func (p *TracePropagatingPublisherDecorator) Close() error {
	return p.pub.Close()
}
