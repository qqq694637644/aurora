package chatgpt

import (
    "aurora/httpclient"
    "aurora/internal/accounts"
    "io"
    stdhttp "net/http"
    stdhttptest "net/http/httptest"
    "strings"
    "testing"
    "time"

    http "github.com/bogdanfinn/fhttp"
    "github.com/bogdanfinn/fhttp/httptest"
    "github.com/bogdanfinn/websocket"
    "github.com/gin-gonic/gin"
)

type protocolLocalErrorClient struct {
    calls int
    status int
}

func (c *protocolLocalErrorClient) Request(_ httpclient.HttpMethod, _ string, _ httpclient.AuroraHeaders, _ []*stdhttp.Cookie, _ io.Reader) (*stdhttp.Response,error) {
    c.calls++
    return &stdhttp.Response{StatusCode:c.status, Body:io.NopCloser(strings.NewReader(`{"detail":"synthetic upstream error"}`))},nil
}
func (c *protocolLocalErrorClient) SetProxy(string) error {return nil}
func (c *protocolLocalErrorClient) SetCookies(string,[]*stdhttp.Cookie) {}
func (c *protocolLocalErrorClient) GetCookies(string) []*stdhttp.Cookie {return nil}

// Local-only 429 / 503 propagation; never sends traffic to a real service.
func TestProtocolLocalPrepareRateLimitAndServerError(t *testing.T) {
    for _, status := range []int{429, 503} {
        client := &protocolLocalErrorClient{status:status}
        _,err:=PrepareConversationConduitWithState(client,chatGPTRequestForTest(),
            &accounts.Account{Type:accounts.TypeNoAuth},"","local-trace",nil)
        if err==nil {t.Fatalf("status %d must fail preparation",status)}
        if client.calls!=1 {t.Fatalf("status %d, got %d attempts; want one",status,client.calls)}
        gin.SetMode(gin.TestMode)
        writer:=stdhttptest.NewRecorder()
        context,_:=gin.CreateTestContext(writer)
        response:=&stdhttp.Response{StatusCode:status,Status:stdhttp.StatusText(status),
            Body:io.NopCloser(strings.NewReader(`{"detail":"synthetic upstream error"}`))}
        if !Handle_request_error(context,response) {t.Fatalf("status %d not classified as error",status)}
        if writer.Code!=status {t.Fatalf("status %d mapped to %d",status,writer.Code)}
    }
}

// Local synthetic WebSocket stream. No ChatGPT cookies, challenge tokens,
// external HTTP requests or private endpoint traffic are involved.
func TestProtocolLocalWebSocketTopicStreamAndDone(t *testing.T) {
    const topic = "local-synthetic-topic"
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil { return }
        defer conn.Close()
        _ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
        _, command, err := conn.ReadMessage()
        if err != nil || !strings.Contains(string(command), topic) {
            return
        }
        events := []string{
            "data: {\"v\":\"WS_LOCAL_OK\"}\n",
            "data: [DONE]\n",
        }
        for _, event := range events {
            frame := map[string]interface{}{
                "type": "message",
                "topic_id": topic,
                "payload": map[string]interface{}{
                    "payload": map[string]interface{}{"encoded_item": event},
                },
            }
            if err := conn.WriteJSON(frame); err != nil { return }
        }
    }))
    defer server.Close()

    wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
    conn, _, err := (&websocket.Dialer{HandshakeTimeout: 4 * time.Second}).Dial(wsURL, nil)
    if err != nil { t.Fatalf("Dial local WebSocket: %v", err) }
    defer conn.Close()

    reader, err := chatWebsocketStreamReader(conn, topic)
    if err != nil { t.Fatalf("streamReader: %v", err) }
    defer reader.Close()
    type response struct { text string; err error }
    completed := make(chan response, 1)
    go func() { body, e := io.ReadAll(reader); completed <- response{string(body), e} }()
    select {
    case result := <-completed:
        if result.err != nil { t.Fatalf("stream read: %v", result.err) }
        if !strings.Contains(result.text, "WS_LOCAL_OK") {
            t.Fatalf("synthetic message missing; len=%d", len(result.text))
        }
        if !strings.Contains(result.text, "data: [DONE]") {
            t.Fatal("synthetic WebSocket stream lacks DONE")
        }
    case <-time.After(6 * time.Second):
        t.Fatal("WebSocket stream did not complete")
    }
}

// The Aurora response handler actually switches from HTTP SSE to a local
// synthetic WebSocket after observing a stream_handoff event and HTTP DONE.
// This is *not* evidence that the current ChatGPT website issued a handoff.
func TestProtocolLocalHTTPToWebSocketHandoff(t *testing.T) {
    const topic = "local-handoff-topic"
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil { return }
        defer conn.Close()
        _ = conn.SetReadDeadline(time.Now().Add(5*time.Second))
        _, _, err = conn.ReadMessage()
        if err != nil { return }
        events := []string{
            "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"LOCAL_HANDOFF_OK\"}\n",
            "data: {\"p\":\"/message/end_turn\",\"o\":\"replace\",\"v\":true}\n",
            "data: [DONE]\n",
        }
        for _, encoded := range events {
            if err := conn.WriteJSON(map[string]interface{}{
                "type": "message", "topic_id": topic,
                "payload": map[string]interface{}{
                    "payload": map[string]interface{}{"encoded_item":encoded},
                },
            }); err != nil { return }
        }
    }))
    defer server.Close()
    conn, _, err := (&websocket.Dialer{HandshakeTimeout: 4*time.Second}).Dial(
        "ws"+strings.TrimPrefix(server.URL,"http"),nil)
    if err != nil { t.Fatalf("local dial failed: %v",err) }
    defer conn.Close()

    httpBody := "event: stream_handoff\n" +
        "data: {\"type\":\"stream_handoff\",\"options\":[{\"type\":\"subscribe_ws_topic\",\"topic_id\":\"" + topic + "\"}]}\n\n" +
        "data: [DONE]\n\n"
    source := &stdhttp.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(httpBody))}
    gin.SetMode(gin.TestMode)
    writer := stdhttptest.NewRecorder()
    context, _ := gin.CreateTestContext(writer)
    got := HandlerDetailedWithOptions(context, source, nil, nil, "local-handoff-id",
        chatGPTRequestForTest(), true, "auto", HandlerDetailedOptions{Websocket: conn})
    if got.Text != "LOCAL_HANDOFF_OK" {
        t.Fatalf("Aurora WS handoff reconstructed %q; want LOCAL_HANDOFF_OK",got.Text)
    }
    if !strings.Contains(writer.Body.String(),"LOCAL_HANDOFF_OK") {
        t.Fatal("Aurora HTTP compatibility stream did not emit the WS-delivered content")
    }
}

func TestProtocolLocalWebSocketUnexpectedDisconnect(t *testing.T) {
    const topic = "local-synthetic-disconnect"
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil { return }
        _ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
        _, _, err = conn.ReadMessage()
        if err != nil { _ = conn.Close(); return }
        _ = conn.WriteJSON(map[string]interface{}{
            "type": "message", "topic_id": topic,
            "payload": map[string]interface{}{
                "payload": map[string]interface{}{
                    "encoded_item": "data: {\"v\":\"PARTIAL\"}\n",
                },
            },
        })
        // Forced local network disconnect, before the application DONE.
        _ = conn.Close()
    }))
    defer server.Close()
    conn, _, err := (&websocket.Dialer{HandshakeTimeout: 4 * time.Second}).Dial(
        "ws" + strings.TrimPrefix(server.URL, "http"), nil)
    if err != nil { t.Fatalf("Dial: %v", err) }
    defer conn.Close()

    reader, err := chatWebsocketStreamReader(conn, topic)
    if err != nil { t.Fatalf("streamReader: %v", err) }
    defer reader.Close()
    type response struct { text string; err error }
    complete := make(chan response, 1)
    go func() { b, e := io.ReadAll(reader); complete <- response{string(b), e} }()
    select {
    case got := <-complete:
        if got.err == nil { t.Fatal("unexpected EOF: connection loss not propagated as error") }
        if strings.Contains(got.text, "data: [DONE]") { t.Fatal("DONE fabricated on disconnect") }
    case <-time.After(6 * time.Second):
        t.Fatal("stream did not report remote disconnect")
    }
}
