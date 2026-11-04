package chatgpt

import (
    "os"
    "sync"
)

// recordProtocolProbeEvent is opt-in, local-only diagnostics for protocol
// validation. It writes only fixed, non-sensitive state names. It never
// records credentials, URLs, topic IDs, message IDs, or conversation text.
// Empty AURORA_PROTOCOL_PROBE_FILE (the default) disables all disk writes.
var protocolProbeFileMu sync.Mutex

func recordProtocolProbeEvent(kind string) {
    switch kind {
    case "websocket_connected", "websocket_handoff_seen", "websocket_handoff_switched":
    default:
        return
    }
    path := os.Getenv("AURORA_PROTOCOL_PROBE_FILE")
    if path == "" {
        return
    }
    protocolProbeFileMu.Lock()
    defer protocolProbeFileMu.Unlock()
    file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    if err != nil {
        return
    }
    defer file.Close()
    _, _ = file.WriteString(kind + "\n")
}
