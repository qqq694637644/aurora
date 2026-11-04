package accounts

import (
    "encoding/json"
    "io"
    stdhttp "net/http"
    "strings"
    "sync/atomic"
    "testing"
    "time"

    "aurora/httpclient"
    http "github.com/bogdanfinn/fhttp"
    "github.com/bogdanfinn/fhttp/httptest"
    "github.com/bogdanfinn/websocket"
)

type protocolLocalFakeActorHTTP struct { websocketURL string }

func (f *protocolLocalFakeActorHTTP) Request(method httpclient.HttpMethod, endpoint string, headers httpclient.AuroraHeaders, cookies []*stdhttp.Cookie, body io.Reader) (*stdhttp.Response, error) {
    payload, _ := json.Marshal(map[string]string{"websocket_url": f.websocketURL})
    return &stdhttp.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(payload)))}, nil
}
func (f *protocolLocalFakeActorHTTP) SetProxy(string) error { return nil }
func (f *protocolLocalFakeActorHTTP) SetCookies(string, []*stdhttp.Cookie) {}
func (f *protocolLocalFakeActorHTTP) GetCookies(string) []*stdhttp.Cookie { return nil }

// Exercises Aurora's actual WSSActor, including a real local TCP/WebSocket
// disconnect and a second successful connection. No Internet calls or real
// user credentials: the HTTP client returns an in-process loopback ws:// URL.
func TestProtocolLocalWSSActorReconnectAfterServerDisconnect(t *testing.T) {
    var connected atomic.Int32
    secondConnected := make(chan struct{}, 1)
    keepAlive := make(chan struct{})

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil { return }
        defer conn.Close()
        attempt := connected.Add(1)
        _ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
        _, raw, err := conn.ReadMessage()
        if err != nil || !strings.Contains(string(raw), "calpico-chatgpt") { return }
        for id := 1; id <= 4; id++ {
            if err := conn.WriteJSON(map[string]interface{}{"type":"reply", "id":id}); err != nil { return }
        }
        if attempt == 1 {
            time.Sleep(120 * time.Millisecond)
            // Simulate a remote close without an application completion marker.
            return
        }
        select {
        case secondConnected <- struct{}{}:
        default:
        }
        select {
        case <-keepAlive:
        case <-time.After(5 * time.Second):
        }
    }))
    defer server.Close()
    defer close(keepAlive)

    account := NewAccount("local-only", TypePUID, "local-placeholder-not-real")
    account.Client = &protocolLocalFakeActorHTTP{
        websocketURL: "ws" + strings.TrimPrefix(server.URL, "http"),
    }
    actor := NewWSSActor(account)
    defer actor.Stop()
    actor.Start()

    select {
    case <-secondConnected:
        if got := connected.Load(); got < 2 {
            t.Fatalf("connected %d times, expected >= 2", got)
        }
    case <-time.After(6 * time.Second):
        t.Fatalf("Aurora WSSActor failed to reconnect; connections=%d", connected.Load())
    }
}
