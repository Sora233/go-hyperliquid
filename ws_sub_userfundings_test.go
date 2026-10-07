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

func TestWsUserFundingsUnmarshal(t *testing.T) {
	raw := []byte(`{
		"isSnapshot": true,
		"user": "0xabc",
		"fundings": [
			{
				"time": 1700000000000,
				"coin": "BTC",
				"usdc": "-1.25",
				"szi": "0.01",
				"fundingRate": "0.0001",
				"nSamples": null
			},
			{
				"time": 1700003600000,
				"coin": "ETH",
				"usdc": "0.5",
				"szi": "-2",
				"fundingRate": "-0.0002",
				"nSamples": 3
			}
		]
	}`)

	var got WsUserFundings
	require.NoError(t, json.Unmarshal(raw, &got))
	require.True(t, got.IsSnapshot)
	require.Equal(t, "0xabc", got.User)
	require.Len(t, got.Fundings, 2)
	require.Nil(t, got.Fundings[0].NSamples)
	require.NotNil(t, got.Fundings[1].NSamples)
	require.Equal(t, 3, *got.Fundings[1].NSamples)
	require.Equal(t, "userFundings:0xabc", got.Key())
}

func TestUserFundingsSubscription(t *testing.T) {
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
		if !ok || sub["type"] != ChannelUserFundings || sub["user"] != user {
			return
		}

		payload, err := json.Marshal(map[string]any{
			"channel": ChannelUserFundings,
			"data": WsUserFundings{
				IsSnapshot: true,
				User:       user,
				Fundings: []WsUserFunding{{
					Time:        1700000000000,
					Coin:        "BTC",
					USDC:        "-1.25",
					Szi:         "0.01",
					FundingRate: "0.0001",
				}},
			},
		})
		if err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, payload)

		// 保持连接，直到客户端关闭。
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
		msg WsUserFundings
		err error
	}
	got := make(chan result, 1)
	sub, err := client.UserFundings(UserFundingsSubscriptionParams{User: user}, func(msg WsUserFundings, err error) {
		got <- result{msg: msg, err: err}
	})
	require.NoError(t, err)
	defer sub.Close()

	select {
	case res := <-got:
		require.NoError(t, res.err)
		require.True(t, res.msg.IsSnapshot)
		require.Equal(t, user, res.msg.User)
		require.Equal(t, []WsUserFunding{{
			Time:        1700000000000,
			Coin:        "BTC",
			USDC:        "-1.25",
			Szi:         "0.01",
			FundingRate: "0.0001",
		}}, res.msg.Fundings)
	case <-ctx.Done():
		t.Fatal("timed out waiting for userFundings")
	}
}
