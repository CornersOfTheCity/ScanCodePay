package db

import (
	"ScanCodePay/internal/models"
	"log"
	"strings"

	"gorm.io/gorm"
)

var DB *gorm.DB // 在 main 中赋值

// 示例：获取地址列表
func GetAddresses(db *gorm.DB) ([]models.Address, error) {
	var addrs []models.Address
	return addrs, db.Find(&addrs).Error
}

// 示例：保存交易
func SaveTransaction(db *gorm.DB, tx *models.Transaction) error {
	return db.Save(tx).Error
}

// 示例：根据订单 ID 查询交易
func GetTransactionByOrderID(db *gorm.DB, orderID string) (*models.Transaction, error) {
	var tx models.Transaction
	err := db.Where("order_id = ?", orderID).First(&tx).Error
	return &tx, err
}

// 保存退款交易
func SaveRefundTransaction(db *gorm.DB, refund *models.RefundTransaction) error {
	return db.Save(refund).Error
}

// 根据退款订单ID查询退款交易
func GetRefundTransactionByRefundOrderID(db *gorm.DB, refundOrderID string) (*models.RefundTransaction, error) {
	var refund models.RefundTransaction
	err := db.Where("refund_order_id = ?", refundOrderID).First(&refund).Error
	return &refund, err
}

// MigrateMemoToOrderID 将数据库中已有记录的 Memo 提取为 OrderID
// 仅处理 OrderID 为空且有 Memo 的记录
func MigrateMemoToOrderID(db *gorm.DB) error {
	// 查询所有 OrderID 为空且 Memo 不为空的记录
	type OldTransaction struct {
		gorm.Model
		OrderID     string `gorm:"uniqueIndex;size:100"`
		Memo        string `gorm:"size:255"`
		Address     string `gorm:"size:44"`
		Amount      uint64
		TXSignature string `gorm:"size:88"`
		BlockHeight uint64
		Status      string `gorm:"size:20;default:'pending'"`
	}

	var txs []OldTransaction
	if err := db.Where("(order_id = '' OR order_id IS NULL) AND memo != '' AND memo IS NOT NULL").Find(&txs).Error; err != nil {
		return err
	}

	if len(txs) == 0 {
		log.Println("没有需要迁移的记录")
		return nil
	}

	log.Printf("找到 %d 条需要迁移的记录，开始迁移...", len(txs))

	updatedCount := 0
	for _, tx := range txs {
		// 按照 listener.go 中的逻辑提取 orderID
		orderID := strings.TrimPrefix(tx.Memo, "order:")

		// 如果 memo 不是以 "order:" 开头，跳过（保持与 listener.go 逻辑一致）
		if orderID == tx.Memo {
			log.Printf("跳过记录 ID %d：Memo '%s' 不以 'order:' 开头", tx.ID, tx.Memo)
			continue
		}

		// 检查是否已存在相同的 OrderID
		var exists OldTransaction
		if err := db.Where("order_id = ?", orderID).First(&exists).Error; err == nil {
			log.Printf("跳过记录 ID %d：OrderID '%s' 已存在", tx.ID, orderID)
			continue
		}

		// 更新 OrderID
		if err := db.Model(&OldTransaction{}).Where("id = ?", tx.ID).Update("order_id", orderID).Error; err != nil {
			log.Printf("更新记录 ID %d 失败: %v", tx.ID, err)
			continue
		}

		updatedCount++
		log.Printf("成功迁移记录 ID %d: Memo '%s' -> OrderID '%s'", tx.ID, tx.Memo, orderID)
	}

	log.Printf("迁移完成：成功更新 %d 条记录", updatedCount)
	return nil
}
