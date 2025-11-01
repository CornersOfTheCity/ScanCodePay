package models

import "gorm.io/gorm"

type Address struct {
	gorm.Model
	Address    string `gorm:"uniqueIndex;size:44"` // Solana 地址长度
	Type       string `gorm:"size:10"`             // "collection" 或 "refund"
	ScanHeight uint64 `gorm:"default:0"`
}

type Transaction struct {
	gorm.Model
	OrderID     string `gorm:"uniqueIndex;size:100"` // 订单 ID（从 Memo 提取）
	Address     string `gorm:"size:44"`
	Amount      uint64 // USDC 金额（lamports）
	TXSignature string `gorm:"size:88"` // 交易签名
	BlockHeight uint64
	Status      string `gorm:"size:20;default:'pending'"` // "pending", "confirmed", "failed"
	Type        string `gorm:"size:20;default:'payment'"`  // "payment" 收款, "refund" 退款
}

// RefundTransaction 退款交易表
type RefundTransaction struct {
	gorm.Model
	RefundOrderID  string `gorm:"uniqueIndex;size:100"` // 退款订单ID（唯一）
	OriginalOrderID string `gorm:"index;size:100"`       // 原收款订单ID
	RefundTo       string `gorm:"size:44"`              // 退款收款人地址
	Amount         uint64                                // 退款金额（lamports）
	TXSignature    string `gorm:"size:88"`              // 交易签名
	BlockHeight    uint64
	Status         string `gorm:"size:20;default:'pending'"` // "pending", "confirmed", "failed"
}
