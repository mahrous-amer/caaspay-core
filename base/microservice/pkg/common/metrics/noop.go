package metrics

import "time"

type NoopMetrics struct{}

func (n *NoopMetrics) Increment(_ string, _ ...int64)         {}
func (n *NoopMetrics) RecordTiming(_ string, _ time.Duration) {}
func (n *NoopMetrics) Shutdown()                              {}
