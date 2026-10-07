package hyperliquid

import "fmt"

type AllDexsClearinghouseStateSubscriptionParams struct {
	User string
}

// AllDexsClearinghouseState 订阅指定用户在所有 dex 上的永续账户状态。
func (w *WebsocketClient) AllDexsClearinghouseState(
	params AllDexsClearinghouseStateSubscriptionParams,
	callback func(WsAllDexsClearinghouseState, error),
) (*Subscription, error) {
	payload := remoteAllDexsClearinghouseStateSubscriptionPayload{
		Type: ChannelAllDexsClearinghouseState,
		User: params.User,
	}

	return w.subscribe(payload, func(msg any) {
		state, ok := msg.(WsAllDexsClearinghouseState)
		if !ok {
			callback(WsAllDexsClearinghouseState{}, fmt.Errorf("invalid message type"))
			return
		}

		callback(state, nil)
	})
}
