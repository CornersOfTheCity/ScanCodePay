package db

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
}
