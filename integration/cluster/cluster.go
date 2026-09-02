// Package cluster starts and stops the real system with Docker Compose. The
// integration tests talk to the same images that get deployed, over the same
// ports, so nothing here is a stand-in for the real thing.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Cluster struct {
	dir     string // the directory holding docker-compose.yml
	project string
	port    string
	dataDir string
	BaseURL string
	verbose bool
}

// New locates the compose file by walking up from the working directory, so the
// tests run the same whether they are started from the repo root or from here.
func New() (*Cluster, error) {
	dir, err := findComposeDir()
	if err != nil {
		return nil, err
	}
	port := envOr("RYUK_IT_PORT", "8091")
	return &Cluster{
		dir:     dir,
		project: envOr("RYUK_IT_PROJECT", "ryuk-it"),
		port:    port,
		dataDir: envOr("RYUK_IT_DATA", "../data-it"),
		BaseURL: envOr("RYUK_BASE_URL", "http://localhost:"+port),
		verbose: os.Getenv("RYUK_IT_VERBOSE") != "",
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func findComposeDir() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for i := 0; i < 6; i++ {
		p := filepath.Join(dir, "deploy", "docker-compose.yml")
		if _, err := os.Stat(p); err == nil {
			return filepath.Join(dir, "deploy"), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("could not find deploy/docker-compose.yml above the working directory")
}

// env keeps the suite's stack away from a development one: its own project
// name, its own port, and its own data directory.
func (c *Cluster) env() []string {
	return append(os.Environ(),
		"COMPOSE_PROJECT_NAME="+c.project,
		"RYUK_PORT="+c.port,
		"RYUK_DATA="+c.dataDir,
	)
}

func (c *Cluster) cmd(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose"}, args...)...)
	cmd.Dir = c.dir
	cmd.Env = c.env()
	return cmd
}

func (c *Cluster) compose(ctx context.Context, args ...string) error {
	cmd := c.cmd(ctx, args...)
	if c.verbose {
		cmd.Stdout = os.Stdout
	}
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (c *Cluster) composeOut(ctx context.Context, args ...string) (string, error) {
	out, err := c.cmd(ctx, args...).Output()
	return string(out), err
}

// Up builds the images and brings the stack up with the requested node count.
// Building is skipped when RYUK_IT_NO_BUILD is set, which makes a rerun quick
// while iterating on the tests themselves.
func (c *Cluster) Up(ctx context.Context, nodes int) error {
	if os.Getenv("RYUK_IT_NO_BUILD") == "" {
		if err := c.compose(ctx, "build"); err != nil {
			return fmt.Errorf("docker compose build: %w", err)
		}
	}
	if err := c.compose(ctx, "up", "-d", "--wait", "--scale", fmt.Sprintf("node=%d", nodes)); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}
	return c.WaitReady(ctx, nodes, 90*time.Second)
}

// Scale changes the node count and waits for the gateway to see them. Adding a
// node is how rebalancing gets exercised.
func (c *Cluster) Scale(ctx context.Context, nodes int) error {
	if err := c.compose(ctx, "up", "-d", "--no-recreate", "--scale", fmt.Sprintf("node=%d", nodes)); err != nil {
		return fmt.Errorf("scale to %d: %w", nodes, err)
	}
	return c.WaitReady(ctx, nodes, 60*time.Second)
}

// StopNode stops one node container without removing it, which is what a
// machine going down looks like. Returns the container name so it can be
// started again.
func (c *Cluster) StopNode(ctx context.Context, index int) (string, error) {
	name, err := c.nodeContainer(ctx, index)
	if err != nil {
		return "", err
	}
	stop := exec.CommandContext(ctx, "docker", "stop", name)
	stop.Stderr = os.Stderr
	if err := stop.Run(); err != nil {
		return "", fmt.Errorf("stop %s: %w", name, err)
	}
	return name, nil
}

func (c *Cluster) StartContainer(ctx context.Context, name string) error {
	start := exec.CommandContext(ctx, "docker", "start", name)
	start.Stderr = os.Stderr
	return start.Run()
}

func (c *Cluster) nodeContainer(ctx context.Context, index int) (string, error) {
	out, err := c.composeOut(ctx, "ps", "-q", "node")
	if err != nil {
		return "", err
	}
	ids := strings.Fields(out)
	if index >= len(ids) {
		return "", fmt.Errorf("node %d requested, only %d running", index, len(ids))
	}
	return ids[index], nil
}

// WaitReady blocks until the gateway answers and reports at least the expected
// number of members. Nodes register themselves, so the count is what proves the
// stack is actually usable rather than merely started.
func (c *Cluster) WaitReady(ctx context.Context, nodes int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		n, err := c.LiveNodes(ctx)
		if err == nil && n >= nodes {
			return nil
		}
		last = err
		if err == nil {
			last = fmt.Errorf("%d of %d nodes registered", n, nodes)
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("cluster not ready after %s: %v", timeout, last)
}

func (c *Cluster) LiveNodes(ctx context.Context) (int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+"/v1/cluster", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer acme-token")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return 0, fmt.Errorf("cluster returned %d", res.StatusCode)
	}
	var body struct {
		Nodes []struct{} `json:"nodes"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return 0, err
	}
	return len(body.Nodes), nil
}

// Down removes the containers and their volumes. Called once at the end so a
// failed run does not leave a half-configured stack behind.
func (c *Cluster) Down(ctx context.Context) error {
	err := c.compose(ctx, "down", "-v", "--remove-orphans")
	// The data directory is a bind mount, so compose does not remove it and the
	// next run would replay another run's write-ahead logs.
	_ = os.RemoveAll(filepath.Join(c.dir, c.dataDir))
	return err
}

// Logs returns recent output from one service, for reporting a failure.
func (c *Cluster) Logs(ctx context.Context, service string, lines int) string {
	out, _ := c.composeOut(ctx, "logs", "--tail", fmt.Sprint(lines), service)
	return out
}
