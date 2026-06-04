// Package telemetry provides utilities for centralized observability, including
// structured logging and distributed tracing. It standardizes how information
// is reported across the various components of the gomigrate system.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/dinocodesx/gomigrate"

// ShutdownFunc is a function that flushes and closes the tracing provider.
type ShutdownFunc func(ctx context.Context) error

// InitTracer initialises an OTLP/HTTP TracerProvider and sets it as the
// global OpenTelemetry tracer.
//
//   - endpoint: the OTLP HTTP endpoint (e.g. "http://localhost:4318"). If empty,
//     a no-op tracer is installed.
//   - serviceName: the service.name resource attribute value (e.g. "gomigrate").
//
// Returns a ShutdownFunc that must be called before process exit to ensure all
// spans are flushed.
func InitTracer(ctx context.Context, endpoint, serviceName string) (ShutdownFunc, error) {
	if endpoint == "" {
		// No-op: leave the global tracer as the default no-op implementation.
		return func(_ context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(), // TLS termination is left to the operator / sidecar proxy.
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(Version),
		),
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithHost(),
	)
	if err != nil {
		// Non-fatal — fall back to a minimal resource.
		res = resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns the global tracer for this library.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// Version is exported so tracer.go can reference the application version
// without an import cycle. It is set by the CLI package via SetVersion.
var Version = "dev"

// SetVersion allows the CLI layer to inject the application version into the
// telemetry package before initialising the tracer.
func SetVersion(v string) {
	Version = v
}
