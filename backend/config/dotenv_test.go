package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseEnvLine(t *testing.T) {
	cases := []struct {
		line, key, value string
		ok               bool
	}{
		{`RYUK_ETCD=localhost:2379`, "RYUK_ETCD", "localhost:2379", true},
		{`  RYUK_LISTEN = :9090  `, "RYUK_LISTEN", ":9090", true},
		{`export RYUK_LOG_LEVEL=debug`, "RYUK_LOG_LEVEL", "debug", true},

		// a password may contain the characters a naive parser would eat
		{`RYUK_POSTGRES="postgres://u:p#w@host/db?sslmode=disable"`,
			"RYUK_POSTGRES", "postgres://u:p#w@host/db?sslmode=disable", true},
		{`A='single # quoted'`, "A", "single # quoted", true},
		{`A=plain value # trailing comment`, "A", "plain value", true},
		{`A="with \n escape"`, "A", "with \n escape", true},

		{``, "", "", false},
		{`# a comment`, "", "", false},
		{`   `, "", "", false},
		{`no equals sign`, "", "", false},
		{`=novalue`, "", "", false},
		{`A="unterminated`, "", "", false},
	}
	for _, c := range cases {
		k, v, ok := parseEnvLine(c.line)
		if ok != c.ok || (ok && (k != c.key || v != c.value)) {
			t.Errorf("parseEnvLine(%q) = %q, %q, %v; want %q, %q, %v",
				c.line, k, v, ok, c.key, c.value, c.ok)
		}
	}
}

// A variable already in the environment must survive: a run configuration or a
// shell override has to beat a checked-out file, or the file cannot be shared.
func TestRealEnvironmentWins(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, ".env"), "RYUK_FROM_FILE=file\nRYUK_OVERRIDDEN=file\n")

	t.Setenv("RYUK_OVERRIDDEN", "real")
	chdir(t, dir)

	applyEnvFile(".env")

	if got := os.Getenv("RYUK_FROM_FILE"); got != "file" {
		t.Fatalf("a value only in the file should be applied, got %q", got)
	}
	if got := os.Getenv("RYUK_OVERRIDDEN"); got != "real" {
		t.Fatalf("the environment should win, got %q", got)
	}
}

// Precedence, end to end: explicit file, then the service's own, then shared,
// with the real environment ahead of all three.
func TestPrecedence(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, ".env"), "RYUK_LISTEN=:8080\nRYUK_SHARED=yes\nRYUK_ALL=shared\n")
	write(t, filepath.Join(dir, ".env.node"), "RYUK_LISTEN=:9110\nRYUK_ALL=service\n")
	write(t, filepath.Join(dir, "explicit.env"), "RYUK_ALL=explicit\nRYUK_REAL=fromfile\n")
	chdir(t, dir)

	for _, k := range []string{"RYUK_LISTEN", "RYUK_SHARED", "RYUK_ALL"} {
		os.Unsetenv(k)
	}
	t.Setenv("RYUK_ENV_FILE", "explicit.env")
	t.Setenv("RYUK_REAL", "fromenv")

	loadDotenv("node")

	for _, c := range []struct{ key, want, why string }{
		{"RYUK_REAL", "fromenv", "the real environment beats every file"},
		{"RYUK_ALL", "explicit", "RYUK_ENV_FILE beats both defaults"},
		{"RYUK_LISTEN", ":9110", "the service's own file beats the shared one"},
		{"RYUK_SHARED", "yes", "the shared file still fills what nothing else set"},
	} {
		if got := os.Getenv(c.key); got != c.want {
			t.Errorf("%s = %q, want %q -- %s", c.key, got, c.want, c.why)
		}
	}
}

func TestMissingFileIsNotAnError(t *testing.T) {
	chdir(t, t.TempDir())
	applyEnvFile(".env")  // must not panic
	loadDotenv("gateway") // nor this
}

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func chdir(t *testing.T, dir string) {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
}
