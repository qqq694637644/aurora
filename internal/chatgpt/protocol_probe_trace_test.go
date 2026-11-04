package chatgpt

import (
    "os"
    "path/filepath"
    "strings"
    "testing"
)

func TestProtocolProbeTraceIsExplicitAndRedacted(t *testing.T) {
    path := filepath.Join(t.TempDir(), "only-protocol-events.txt")
    t.Setenv("AURORA_PROTOCOL_PROBE_FILE", "")
    recordProtocolProbeEvent("websocket_connected")
    if _, err := os.Stat(path); !os.IsNotExist(err) {
        t.Fatalf("probe should not write when disabled")
    }
    t.Setenv("AURORA_PROTOCOL_PROBE_FILE", path)
    recordProtocolProbeEvent("conversation_id=secret")
    recordProtocolProbeEvent("websocket_connected")
    recordProtocolProbeEvent("websocket_handoff_seen")
    recordProtocolProbeEvent("websocket_handoff_switched")
    data, err := os.ReadFile(path)
    if err != nil { t.Fatal(err) }
    text := string(data)
    if strings.Contains(text, "secret") || strings.Contains(text, "conversation_id") {
        t.Fatal("sensitive or unknown probe event was persisted")
    }
    if strings.Count(text, "\n") != 3 {
        t.Fatalf("expected three fixed events, got %d", strings.Count(text, "\n"))
    }
}
