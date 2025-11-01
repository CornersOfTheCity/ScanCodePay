package models

// SponsorTxRequest 用户签名交易请求（用于 sign-tx）
type SponsorTxRequest struct {
	SerializedTx string `json:"serializedTx" binding:"required"`
}

// BroadcastTxRequest 已签名交易广播请求
type BroadcastTxRequest struct {
	SerializedTx string `json:"serializedTx" binding:"required"` // 已签名序列化 TX
}

// SignTxResponse 签名响应（返回已签名 TX）
type SignTxResponse struct {
	SerializedSignedTx string `json:"serializedSignedTx"` // Base64 已签名 TX
	SignatureCount     int    `json:"signatureCount"`     // 签名数量（调试用）
}

// BroadcastTxResponse 广播响应
type BroadcastTxResponse struct {
	Signature   string `json:"signature"`
	ExplorerUrl string `json:"explorerUrl"`
}
