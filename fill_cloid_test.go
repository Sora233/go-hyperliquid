package hyperliquid

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFillKeepsCloid(t *testing.T) {
	const raw = `{"coin":"BTC","px":"86328.0","sz":"0.00017","side":"B","time":1,"oid":61978919420,"cloid":"0x7b15dfdd080fca5a8899c5d654fa0c26","crossed":true,"fee":"0.006604","tid":1,"feeToken":"USDC","closedPnl":"0.0","hash":"0xabc","startPosition":"0.0","dir":"Open Long"}`

	var fill Fill
	require.NoError(t, json.Unmarshal([]byte(raw), &fill))
	require.NotNil(t, fill.Cloid)
	require.Equal(t, "0x7b15dfdd080fca5a8899c5d654fa0c26", *fill.Cloid)

	var wsFill WsOrderFill
	require.NoError(t, json.Unmarshal([]byte(raw), &wsFill))
	require.NotNil(t, wsFill.Cloid)
	require.Equal(t, "0x7b15dfdd080fca5a8899c5d654fa0c26", *wsFill.Cloid)

	encoded, err := json.Marshal(fill)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"cloid":"0x7b15dfdd080fca5a8899c5d654fa0c26"`)
}
