package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func num(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func list(key string, def []string) []string {
	if v := os.Getenv(key); v != "" {
		return strings.Split(v, ",")
	}
	return def
}

type Node struct {
	Listen      string
	AdvertiseAs string
	DataDir     string
	WALSync     string
	WALInterval time.Duration
	SweepEvery  time.Duration
	Etcd        []string
	LeaseTTL    int
	LogLevel    string
	LogFormat   string
}

func LoadNode() Node {
	loadDotenv("node")

	n := Node{
		Listen:      str("RYUK_LISTEN", ":9090"),
		AdvertiseAs: str("RYUK_ADVERTISE", ""),
		DataDir:     str("RYUK_DATA_DIR", "./data"),
		WALSync:     str("RYUK_WAL_SYNC", "interval"),
		WALInterval: dur("RYUK_WAL_INTERVAL", 100*time.Millisecond),
		SweepEvery:  dur("RYUK_SWEEP_EVERY", 200*time.Millisecond),
		Etcd:        list("RYUK_ETCD", nil),
		LeaseTTL:    num("RYUK_LEASE_TTL", 10),
		LogLevel:    str("RYUK_LOG_LEVEL", "info"),
		LogFormat:   str("RYUK_LOG_FORMAT", "text"),
	}
	// Replicas share one mounted directory, so each takes a subdirectory named by
	// its container hostname.
	if str("RYUK_DATA_PER_HOST", "") != "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			n.DataDir = filepath.Join(n.DataDir, h)
		}
	}
	return n
}

type Gateway struct {
	Listen       string
	Postgres     string
	Etcd         []string
	CollectEvery time.Duration
	Prometheus   string
	CacheTTL     time.Duration
	BackstopPoll time.Duration
	StaticDir    string
	LogLevel     string
	LogFormat    string
}

func LoadGateway() Gateway {
	loadDotenv("gateway")

	return Gateway{
		Listen:       str("RYUK_LISTEN", ":8080"),
		Postgres:     str("RYUK_POSTGRES", "postgres://ryuk:ryuk@localhost:5432/ryuk?sslmode=disable"),
		Etcd:         list("RYUK_ETCD", []string{"localhost:2379"}),
		CollectEvery: dur("RYUK_COLLECT_EVERY", time.Second),
		Prometheus:   str("RYUK_PROMETHEUS", ""),
		CacheTTL:     dur("RYUK_CACHE_TTL", 30*time.Second),
		BackstopPoll: dur("RYUK_BACKSTOP_POLL", 5*time.Second),
		StaticDir:    str("RYUK_STATIC_DIR", ""),
		LogLevel:     str("RYUK_LOG_LEVEL", "info"),
		LogFormat:    str("RYUK_LOG_FORMAT", "text"),
	}
}
