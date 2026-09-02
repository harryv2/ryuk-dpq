package walfile

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// NodeID is generated once and kept beside the data. A container hostname
// changes on every recreate, and the identity's whole job is to say who holds
// this data, so it has to live with the data.
func NodeID(root string) (string, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	p := filepath.Join(root, "node-id")
	if b, err := os.ReadFile(p); err == nil {
		if id := strings.TrimSpace(string(b)); id != "" {
			return id, nil
		}
	}
	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	id := "node-" + hex.EncodeToString(raw[:])
	return id, os.WriteFile(p, []byte(id), 0o644)
}

// NextIncarnation bumps the restart counter. Lease epochs restart from zero
// after a replay, so without this a receipt from before a crash could match a
// lease handed out after it.
func NextIncarnation(root string) (uint64, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return 0, err
	}
	p := filepath.Join(root, "incarnation")
	cur := uint64(0)
	if b, err := os.ReadFile(p); err == nil {
		if n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil {
			cur = n
		}
	}
	cur++
	return cur, os.WriteFile(p, []byte(strconv.FormatUint(cur, 10)), 0o644)
}
