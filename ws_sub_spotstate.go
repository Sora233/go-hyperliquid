package hyperliquid

import "fmt"

type SpotStateSubscriptionParams struct {
	User string
	// IsPortfolioMargin 对应文档里的可选参数。为空时不发送该字段。
	IsPortfolioMargin *bool
}

// SpotState 订阅指定用户的现货余额。
func (w *WebsocketClient) SpotState(
	params SpotStateSubscriptionParams,
	callback func(WsSpotState, error),
) (*Subscription, error) {
	payload := remoteSpotStateSubscriptionPayload{
		Type:              ChannelSpotState,
		User:              params.User,
		IsPortfolioMargin: params.IsPortfolioMargin,
	}

	return w.subscribe(payload, func(msg any) {
		state, ok := msg.(WsSpotState)
		if !ok {
			callback(WsSpotState{}, fmt.Errorf("invalid message type"))
			return
		}

		callback(state, nil)
	})
}
