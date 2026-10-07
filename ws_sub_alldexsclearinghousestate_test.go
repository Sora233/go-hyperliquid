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

func TestWsAllDexsClearinghouseStateUnmarshal(t *testing.T) {
	raw := []byte(`{
		"user": "0xabc",
		"clearinghouseStates": [
			["", {
				"marginSummary": {"accountValue":"1","totalNtlPos":"0","totalRawUsd":"1","totalMarginUsed":"0"},
				"crossMarginSummary": {"accountValue":"1","totalNtlPos":"0","totalRawUsd":"1","totalMarginUsed":"0"},
				"crossMaintenanceMarginUsed": "0.0",
				"withdrawable": "1",
				"assetPositions": [],
				"time": 1
			}],
			["xyz", {
				"marginSummary": {"accountValue":"2","totalNtlPos":"0","totalRawUsd":"2","totalMarginUsed":"0"},
				"crossMarginSummary": {"accountValue":"2","totalNtlPos":"0","totalRawUsd":"2","totalMarginUsed":"0"},
				"crossMaintenanceMarginUsed": "0.1",
				"withdrawable": "2",
				"assetPositions": [{
					"type": "oneWay",
					"position": {
						"coin": "xyz:TSLA",
						"szi": "1",
						"leverage": {"type": "cross", "value": 5},
						"entryPx": "10",
						"positionValue": "10",
						"unrealizedPnl": "0",
						"returnOnEquity": "0",
						"liquidationPx": null,
						"marginUsed": "2",
						"maxLeverage": 10
					}
				}],
				"time": 2
			}]
		]
	}`)

	var got WsAllDexsClearinghouseState
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, "0xabc", got.User)
	require.Equal(t, "allDexsClearinghouseState:0xabc", got.Key())
	require.Len(t, got.ClearinghouseStates, 2)
	require.Equal(t, "", got.ClearinghouseStates[0].First)
	require.Equal(t, "1", got.ClearinghouseStates[0].Second.Withdrawable)
	require.Equal(t, "xyz", got.ClearinghouseStates[1].First)
	require.Equal(t, "0.1", got.ClearinghouseStates[1].Second.CrossMaintenanceMarginUsed)
	require.Len(t, got.ClearinghouseStates[1].Second.AssetPositions, 1)
	require.Equal(t, 10, got.ClearinghouseStates[1].Second.AssetPositions[0].Position.MaxLeverage)
}

func TestAllDexsClearinghouseStateSubscription(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	const user = "0xabc"

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

		var cmd wsCommand
		if err := json.Unmarshal(msg, &cmd); err != nil {
			return
		}
		sub, ok := cmd.Subscription.(map[string]any)
		if !ok || sub["type"] != ChannelAllDexsClearinghouseState || sub["user"] != user {
			return
		}

		payload, err := json.Marshal(map[string]any{
			"channel": ChannelAllDexsClearinghouseState,
			"data": map[string]any{
				"user": user,
				"clearinghouseStates": []any{
					[]any{"", map[string]any{
						"marginSummary":              map[string]any{},
						"crossMarginSummary":         map[string]any{},
						"crossMaintenanceMarginUsed": "0.0",
						"withdrawable":               "1",
						"assetPositions":             []any{},
						"time":                       1,
					}},
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
		msg WsAllDexsClearinghouseState
		err error
	}
	got := make(chan result, 1)
	sub, err := client.AllDexsClearinghouseState(
		AllDexsClearinghouseStateSubscriptionParams{User: user},
		func(msg WsAllDexsClearinghouseState, err error) {
			got <- result{msg: msg, err: err}
		},
	)
	require.NoError(t, err)
	defer sub.Close()

	select {
	case res := <-got:
		require.NoError(t, res.err)
		require.Equal(t, user, res.msg.User)
		require.Len(t, res.msg.ClearinghouseStates, 1)
		require.Equal(t, "", res.msg.ClearinghouseStates[0].First)
		require.Equal(t, "1", res.msg.ClearinghouseStates[0].Second.Withdrawable)
	case <-ctx.Done():
		t.Fatal("timed out waiting for allDexsClearinghouseState")
	}
}
