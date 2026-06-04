// Package metrics provides Prometheus-based observability for the gomigrate system.
// It defines and initializes counters, gauges, and histograms to track the
// health and performance of migration and backup pipelines.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// RecordsRead tracks the total number of records successfully extracted from source adapters.
	RecordsRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gomigrate_records_read_total",
		Help: "Total number of records read from source",
	}, []string{"table"})

	// RecordsWritten tracks the total number of records successfully ingested into target adapters.
	RecordsWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gomigrate_records_written_total",
		Help: "Total number of records written to target",
	}, []string{"table"})

	// RecordsFailed tracks the number of records that encountered errors during the pipeline.
	RecordsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gomigrate_records_failed_total",
		Help: "Total number of records that failed during migration",
	}, []string{"table"})

	// PartitionsTotal tracks the total number of work units discovered for a specific table.
	PartitionsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gomigrate_partitions_total",
		Help: "Total number of partitions planned",
	}, []string{"table"})

	// PartitionsDone tracks the number of work units that have completed processing.
	PartitionsDone = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gomigrate_partitions_done",
		Help: "Number of partitions completed",
	}, []string{"table"})

	// BatchWriteDuration records the latency of bulk write operations to target databases.
	BatchWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gomigrate_batch_write_duration_seconds",
		Help:    "Latency of batch write operations",
		Buckets: prometheus.DefBuckets,
	}, []string{"table"})
)

// StartMetricsServer initializes and runs an HTTP server to expose Prometheus
// metrics at the specified address. It blocks until the server is stopped.
func StartMetricsServer(addr string) error {
	if addr == "" {
		return nil
	}
	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(addr, nil)
}
