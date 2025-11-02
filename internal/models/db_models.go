package models

import "gorm.io/gorm"

type Transaction struct {
	gorm.Model
	OrderID         string `gorm:"uniqueIndex;size:100"` // 订单 ID（从 Memo 提取）
	ReceiverAddress string `gorm:"size:44"`              // 收款人地址（从交易中提取）
	SenderAddress   string `gorm:"size:44"`              // 付款人地址（从交易中提取）
	Amount          uint64 // USDC 金额（lamports）
	TXSignature     string `gorm:"size:88"` // 交易签名
	BlockHeight     uint64
	Status          string `gorm:"size:20;default:'pending'"` // "pending", "confirmed", "failed"
}

// RefundTransaction 退款交易表
type RefundTransaction struct {
	gorm.Model
	OriginalOrderID string `gorm:"index;size:100"` // 原收款订单ID
	RefundTo        string `gorm:"size:44"`        // 退款收款人地址
	Amount          uint64 // 退款金额（lamports）
	TXSignature     string `gorm:"uniqueIndex;size:88"` // 交易签名（唯一索引）
	BlockHeight     uint64
	Status          string `gorm:"size:20;default:'pending'"` // "pending", "confirmed", "failed"
}
