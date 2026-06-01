package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	RecordsRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gomigrate_records_read_total",
		Help: "Total number of records read from source",
	}, []string{"table"})

	RecordsWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gomigrate_records_written_total",
		Help: "Total number of records written to target",
	}, []string{"table"})

	RecordsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gomigrate_records_failed_total",
		Help: "Total number of records that failed during migration",
	}, []string{"table"})

	PartitionsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gomigrate_partitions_total",
		Help: "Total number of partitions planned",
	}, []string{"table"})

	PartitionsDone = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gomigrate_partitions_done",
		Help: "Number of partitions completed",
	}, []string{"table"})

	BatchWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gomigrate_batch_write_duration_seconds",
		Help:    "Latency of batch write operations",
		Buckets: prometheus.DefBuckets,
	}, []string{"table"})
)

// StartMetricsServer starts a Prometheus metrics server on the given address.
func StartMetricsServer(addr string) error {
	if addr == "" {
		return nil
	}
	http.Handle("/metrics", promhttp.Handler())
	return http.ListenAndServe(addr, nil)
}
