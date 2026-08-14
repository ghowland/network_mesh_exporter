// Package http fetches the inventory document from a URL and keeps a
// local cache, so that a node starts and stays operational when the URL
// is unreachable.
package http

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	nethttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/provider"
	filesrc "github.com/example/mesh/internal/provider/file"
)

// Name is the source identifier.
const Name = "http"

// maxBody bounds the accepted document size.
const maxBody = 64 << 20

// CacheMeta is the sidecar record written beside the cached document.
type CacheMeta struct {
	ETag         string    `json:"etag"`
	LastModified string    `json:"last_modified"`
	FetchedAt    time.Time `json:"fetched_at"`
	URL          string    `json:"url"`
	SHA256       string    `json:"sha256"`
}

// Provider fetches the inventory document over HTTP.
type Provider struct {
	cfg     config.HTTPConfig
	client  *nethttp.Client
	meta    CacheMeta
	refresh chan struct{}
	attempt int
	haveSet bool
}

// New creates an HTTP Provider and builds its TLS configuration.
func New(cfg config.HTTPConfig) (*Provider, error) {
	client, err := buildClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Provider{cfg: cfg, client: client, refresh: make(chan struct{}, 1)}, nil
}

func (p *Provider) Name() string { return Name }

// Refresh requests an immediate fetch.
func (p *Provider) Refresh() {
	select {
	case p.refresh <- struct{}{}:
	default:
	}
}

// Run loads the cache, applies it immediately, then polls the URL. The
// order matters: the node is operational before the first fetch.
func (p *Provider) Run(ctx context.Context, sink provider.Sink) error {
	if set, err := p.loadCache(); err == nil {
		p.haveSet = true
		sink.Replace(set)
		slog.Info("inventory cache loaded", "path", p.cfg.CachePath,
			"hosts", len(set.Hosts), "stale", set.Stale)
	} else if !os.IsNotExist(err) {
		slog.Warn("inventory cache unusable", "path", p.cfg.CachePath, "error", err)
	}

	interval := p.cfg.Interval.D()
	if interval <= 0 {
		interval = 60 * time.Second
	}
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.refresh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}

		set, changed, err := p.fetch(ctx)
		switch {
		case err != nil:
			p.attempt++
			delay := provider.Backoff(p.attempt, p.cfg.BackoffMin.D(), p.cfg.BackoffMax.D())
			slog.Warn("inventory fetch failed, keeping current set",
				"url", p.cfg.URL, "error", err, "retry_in", delay)
			p.reportStale(sink)
			timer.Reset(delay)
			continue
		case changed:
			p.attempt = 0
			p.haveSet = true
			sink.Replace(set)
			slog.Info("inventory fetched", "url", p.cfg.URL, "hosts", len(set.Hosts))
		default:
			p.attempt = 0
			slog.Debug("inventory unchanged", "url", p.cfg.URL)
		}
		timer.Reset(interval)
	}
}

// fetch performs one conditional request. A 200 response replaces the
// set and rewrites the cache. A 304 response keeps the current set and
// does not touch the cache. Any failure keeps the current set.
func (p *Provider) fetch(ctx context.Context) (inventory.Set, bool, error) {
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, p.cfg.URL, nil)
	if err != nil {
		return inventory.Set{}, false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "meshd/1")
	if p.meta.ETag != "" {
		req.Header.Set("If-None-Match", p.meta.ETag)
	}
	if p.meta.LastModified != "" {
		req.Header.Set("If-Modified-Since", p.meta.LastModified)
	}
	auth, err := p.authHeader()
	if err != nil {
		return inventory.Set{}, false, err
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return inventory.Set{}, false, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == nethttp.StatusNotModified {
		return inventory.Set{}, false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return inventory.Set{}, false, fmt.Errorf("http status %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return inventory.Set{}, false, err
	}

	set, err := filesrc.Parse(body, Name, p.cfg.Priority)
	if err != nil {
		return inventory.Set{}, false, err
	}

	sum := sha256.Sum256(body)
	meta := CacheMeta{
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		FetchedAt:    time.Now(),
		URL:          p.cfg.URL,
		SHA256:       hex.EncodeToString(sum[:]),
	}
	if err := p.writeCache(body, meta); err != nil {
		slog.Warn("inventory cache write failed", "path", p.cfg.CachePath, "error", err)
	}
	p.meta = meta
	set.FetchAt = meta.FetchedAt
	return set, true, nil
}

// reportStale re-emits the cached set with the stale flag when the cache
// is older than the configured maximum. Staleness does not clear slots;
// it is reported so that an operator can see the condition.
func (p *Provider) reportStale(sink provider.Sink) {
	if !p.haveSet {
		return
	}
	age, err := p.cacheAge()
	if err != nil || p.cfg.CacheMaxAge.D() <= 0 || age <= p.cfg.CacheMaxAge.D() {
		return
	}
	set, err := p.loadCache()
	if err != nil {
		return
	}
	set.Stale = true
	set.LastError = fmt.Sprintf("cache age %s exceeds cache_max_age", age.Truncate(time.Second))
	sink.Replace(set)
}

// loadCache reads the cached document and its metadata. The returned set
// is marked stale when it is older than CacheMaxAge.
func (p *Provider) loadCache() (inventory.Set, error) {
	data, err := os.ReadFile(p.cfg.CachePath)
	if err != nil {
		return inventory.Set{}, err
	}
	set, err := filesrc.Parse(data, Name, p.cfg.Priority)
	if err != nil {
		return inventory.Set{}, err
	}
	if mb, err := os.ReadFile(metaPath(p.cfg.CachePath)); err == nil {
		var m CacheMeta
		if json.Unmarshal(mb, &m) == nil && m.URL == p.cfg.URL {
			p.meta = m
			set.FetchAt = m.FetchedAt
		}
	}
	if age, err := p.cacheAge(); err == nil &&
		p.cfg.CacheMaxAge.D() > 0 && age > p.cfg.CacheMaxAge.D() {
		set.Stale = true
	}
	return set, nil
}

// writeCache writes the document and the metadata atomically: a
// temporary file in the same directory, fsync, then rename.
func (p *Provider) writeCache(body []byte, meta CacheMeta) error {
	if err := os.MkdirAll(filepath.Dir(p.cfg.CachePath), 0o755); err != nil {
		return err
	}
	if err := writeAtomic(p.cfg.CachePath, body); err != nil {
		return err
	}
	mb, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(metaPath(p.cfg.CachePath), mb)
}

// cacheAge returns the age of the cached document.
func (p *Provider) cacheAge() (time.Duration, error) {
	fi, err := os.Stat(p.cfg.CachePath)
	if err != nil {
		return 0, err
	}
	return time.Since(fi.ModTime()), nil
}

// authHeader returns the Authorization value. The file is re-read on
// every fetch so that a rotated token is picked up without a restart.
func (p *Provider) authHeader() (string, error) {
	if p.cfg.AuthHeaderFile != "" {
		b, err := os.ReadFile(p.cfg.AuthHeaderFile)
		if err != nil {
			return "", fmt.Errorf("read auth header file: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return p.cfg.AuthHeader, nil
}

// buildClient constructs the HTTP client with the timeout and the trust
// store.
func buildClient(cfg config.HTTPConfig) (*nethttp.Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.InsecureTLS {
		tlsCfg.InsecureSkipVerify = true
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file contains no usable certificate")
		}
		tlsCfg.RootCAs = pool
	}
	timeout := cfg.Timeout.D()
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	tr := nethttp.DefaultTransport.(*nethttp.Transport).Clone()
	tr.TLSClientConfig = tlsCfg
	return &nethttp.Client{Timeout: timeout, Transport: tr}, nil
}

// metaPath returns the sidecar path derived from the cache path.
func metaPath(cachePath string) string {
	ext := filepath.Ext(cachePath)
	return strings.TrimSuffix(cachePath, ext) + ".meta" + ext
}

// writeAtomic writes a file through a temporary file in the same
// directory, so that a partial write is never visible.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

