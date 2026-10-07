package hyperliquid

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestWsSpotStateUnmarshal(t *testing.T) {
	// 外层是文档里的 WsSpotState。balances 取自 testdata/SpotUserState.yaml 的 spotClearinghouseState 响应。
	raw := []byte(`{
		"user": "0x8e0C473fed9630906779f982Cd0F80Cb7011812D",
		"spotState": {
			"balances": [
				{"coin":"USDC","token":0,"total":"19.9969993","hold":"0.0","entryNtl":"0.0"},
				{"coin":"HYPE","token":1105,"total":"0.24965","hold":"0.2","entryNtl":"24.982487"},
				{"coin":"USOL","token":1279,"total":"0.9993","hold":"0.0","entryNtl":"249.99"}
			]
		}
	}`)

	var got WsSpotState
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, "0x8e0C473fed9630906779f982Cd0F80Cb7011812D", got.User)
	require.Equal(t, "spotState:0x8e0C473fed9630906779f982Cd0F80Cb7011812D", got.Key())
	require.Equal(t, []SpotBalance{
		{Coin: "USDC", Token: 0, Total: "19.9969993", Hold: "0.0", EntryNtl: "0.0"},
		{Coin: "HYPE", Token: 1105, Total: "0.24965", Hold: "0.2", EntryNtl: "24.982487"},
		{Coin: "USOL", Token: 1279, Total: "0.9993", Hold: "0.0", EntryNtl: "249.99"},
	}, got.SpotState.Balances)
}

func TestSpotStateSubscription(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	const user = "0x8e0C473fed9630906779f982Cd0F80Cb7011812D"
	portfolioMargin := true

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var cmd struct {
			Method       string          `json:"method"`
			Subscription json.RawMessage `json:"subscription"`
		}
		if err := json.Unmarshal(msg, &cmd); err != nil {
			return
		}
		var sub remoteSpotStateSubscriptionPayload
		if err := json.Unmarshal(cmd.Subscription, &sub); err != nil {
			return
		}
		if sub.Type != ChannelSpotState || sub.User != user || sub.IsPortfolioMargin == nil || !*sub.IsPortfolioMargin {
			return
		}

		payload, err := json.Marshal(map[string]any{
			"channel": ChannelSpotState,
			"data": map[string]any{
				"user": user,
				"spotState": map[string]any{
					"balances": []any{
						map[string]any{
							"coin": "USDC", "token": 0, "total": "19.9969993", "hold": "0.0", "entryNtl": "0.0",
						},
					},
				},
			},
		})
		if err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, payload)

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := NewWebsocketClient(server.URL)
	require.NoError(t, client.Connect(ctx))
	defer func() { _ = client.Close() }()

	type result struct {
		msg WsSpotState
		err error
	}
	got := make(chan result, 1)
	sub, err := client.SpotState(SpotStateSubscriptionParams{
		User:              user,
		IsPortfolioMargin: &portfolioMargin,
	}, func(msg WsSpotState, err error) {
		got <- result{msg: msg, err: err}
	})
	require.NoError(t, err)
	defer sub.Close()

	select {
	case res := <-got:
		require.NoError(t, res.err)
		require.Equal(t, user, res.msg.User)
		require.Equal(t, []SpotBalance{{
			Coin: "USDC", Token: 0, Total: "19.9969993", Hold: "0.0", EntryNtl: "0.0",
		}}, res.msg.SpotState.Balances)
	case <-ctx.Done():
		t.Fatal("timed out waiting for spotState")
	}
}
