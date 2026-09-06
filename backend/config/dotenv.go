package config

import (
	"bufio"
	"os"
	"strings"
)

// loadDotenv fills gaps in the environment before the config is built, so a
// local run needs no exported variables. First source to define a variable
// wins: the real environment, then $RYUK_ENV_FILE, then .env.<service>, .env.
func loadDotenv(service string) {
	for _, name := range []string{os.Getenv("RYUK_ENV_FILE"), ".env." + service, ".env"} {
		if name != "" {
			applyEnvFile(name)
		}
	}
}

func applyEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		key, value, ok := parseEnvLine(sc.Text())
		if !ok {
			continue
		}
		// The real environment is the authority; the file only fills gaps.
		if _, set := os.LookupEnv(key); !set {
			_ = os.Setenv(key, value)
		}
	}
}

// parseEnvLine reads one line.
func parseEnvLine(line string) (key, value string, ok bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	line = strings.TrimPrefix(line, "export ")

	key, rest, found := strings.Cut(line, "=")
	if !found {
		return "", "", false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", false
	}

	rest = strings.TrimSpace(rest)
	switch {
	case strings.HasPrefix(rest, `"`):
		// A quoted value may contain '#' and spaces, and honours \n and \t.
		value, ok = unquote(rest, '"')
		if ok {
			value = strings.NewReplacer(`\n`, "\n", `\t`, "\t", `\"`, `"`).Replace(value)
		}
	case strings.HasPrefix(rest, `'`):
		value, ok = unquote(rest, '\'')
	default:
		// Unquoted: everything up to an unescaped comment.
		if i := strings.Index(rest, " #"); i >= 0 {
			rest = rest[:i]
		}
		value, ok = strings.TrimSpace(rest), true
	}
	return key, value, ok
}

func unquote(s string, q byte) (string, bool) {
	if end := strings.IndexByte(s[1:], q); end >= 0 {
		return s[1 : end+1], true
	}
	return "", false // unterminated quote: skip the line rather than guess
}
