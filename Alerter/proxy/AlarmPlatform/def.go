package AlarmPlatform

// timeout is in milliseconds
const DEFAULT_TRANSPORT_TIMEOUT = 5000

// PlatformResp PlatformResp
type PlatformResp struct {
	Msg  string `json:"msg"`
	Data string `json:"data"`
	Code string `json:"code"`
}
