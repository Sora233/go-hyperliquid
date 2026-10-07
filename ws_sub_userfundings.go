package hyperliquid

import "fmt"

type UserFundingsSubscriptionParams struct {
	User string
}

// UserFundings 订阅指定用户的资金费。
func (w *WebsocketClient) UserFundings(
	params UserFundingsSubscriptionParams,
	callback func(WsUserFundings, error),
) (*Subscription, error) {
	payload := remoteUserFundingsSubscriptionPayload{
		Type: ChannelUserFundings,
		User: params.User,
	}

	return w.subscribe(payload, func(msg any) {
		fundings, ok := msg.(WsUserFundings)
		if !ok {
			callback(WsUserFundings{}, fmt.Errorf("invalid message type"))
			return
		}

		callback(fundings, nil)
	})
}
