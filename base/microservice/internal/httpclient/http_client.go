package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	//	"net/url"
	"time"

	"github.com/caaspay/caaspay-core/internal/compliance"
	"github.com/caaspay/caaspay-core/pkg/common/logger"
	"github.com/caaspay/caaspay-core/pkg/common/metrics"
	"github.com/caaspay/caaspay-core/pkg/common/supervisor"
)

type Client struct {
	client     *http.Client
	log        *logger.Logger
	metrics    *metrics.Metrics
	supervisor *supervisor.Supervisor
	compliance *compliance.ComplianceReporter
	userAgent  string
}

type Config struct {
	Timeout    time.Duration
	UserAgent  string
	Logger     *logger.Logger
	Metrics    *metrics.Metrics
	Supervisor *supervisor.Supervisor
	Compliance *compliance.ComplianceReporter
}

func NewClient(cfg Config) *Client {
	return &Client{
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
		log:        cfg.Logger,
		metrics:    cfg.Metrics,
		supervisor: cfg.Supervisor,
		compliance: cfg.Compliance,
		userAgent:  cfg.UserAgent,
	}
}

// RequestHTTPRaw performs a raw HTTP request, returns full http.Response.
func (c *Client) RequestHTTPRaw(ctx context.Context, method, rawURL string, body []byte, headers map[string]string) (*http.Response, []byte, error) {
	start := time.Now()
	c.compliance.TrackEvent("http.request")

	req, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(body))
	if err != nil {
		c.log.Error(ctx, "❌ Failed to create HTTP request", map[string]interface{}{
			"method": method, "url": rawURL, "error": err.Error(),
		})
		return nil, nil, err
	}

	req.Header.Set("User-Agent", c.userAgent)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.client.Do(req)
	duration := time.Since(start)

	tags := []string{"method", method, "url", rawURL}
	if err != nil {
		c.log.Error(ctx, "❌ HTTP request failed", map[string]interface{}{
			"method": method, "url": rawURL, "error": err.Error(),
		})
		c.metrics.IncrementTagged("http.requests.failed", tags...)
		return nil, nil, err
	}

	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, err
	}

	c.metrics.IncrementTagged("http.requests.total", tags...)
	c.metrics.ObserveHistogram("http.request.duration", duration.Seconds(), tags...)

	c.log.Info(ctx, "📡 HTTP request complete", map[string]interface{}{
		"method":   method,
		"url":      rawURL,
		"status":   resp.StatusCode,
		"duration": duration.String(),
	})

	return resp, respBody, nil
}

// RequestHTTP performs an HTTP request with a typed input/output and JSON encoding.
func (c *Client) RequestHTTP(
	ctx context.Context,
	method, baseURL string,
	input any,
	headers map[string]string,
	output any,
) error {
	// Marshal input
	bodyBytes, err := json.Marshal(input)
	if err != nil {
		return err
	}

	// Set headers
	if headers == nil {
		headers = map[string]string{}
	}
	headers["Content-Type"] = "application/json"

	// Make raw request (returns body already read)
	resp, respBody, err := c.RequestHTTPRaw(ctx, method, baseURL, bodyBytes, headers)
	if err != nil {
		return err
	}

	// Decode JSON body into output
	if err := json.Unmarshal(respBody, output); err != nil {
		c.log.Error(ctx, "⚠️ Failed to decode JSON response", map[string]interface{}{
			"url":    baseURL,
			"method": method,
			"status": resp.StatusCode,
			"body":   string(respBody),
		})
		return err
	}

	return nil
}
