package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"ScanCodePay/internal/db"
	"ScanCodePay/internal/models"
)

// GetPaymentHandler 查询收款交易（通过订单ID）
func GetPaymentHandler(c *gin.Context) {
	orderID := c.Param("orderId")

	// 使用全局 DB 连接
	if db.DB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库未初始化"})
		return
	}

	// 查询收款订单
	tx, err := db.GetTransactionByOrderID(db.DB, orderID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "订单未找到"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"order_id":     tx.OrderID,
		"status":       tx.Status,
		"address":      tx.Address,
		"amount":       tx.Amount,
		"tx_signature": tx.TXSignature,
		"block_height": tx.BlockHeight,
		"created_at":   tx.CreatedAt,
	})
}

// GetRefundHandler 查询退款交易（通过订单ID，支持原订单ID或交易签名）
func GetRefundHandler(c *gin.Context) {
	orderID := c.Param("orderId")

	// 使用全局 DB 连接
	if db.DB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库未初始化"})
		return
	}

	var refund *models.RefundTransaction
	var err error

	// 优先尝试作为交易签名查询（交易签名通常是88字符）
	if len(orderID) == 88 {
		refund, err = db.GetRefundTransactionBySignature(db.DB, orderID)
	} else {
		// 作为原订单ID查询
		refund, err = db.GetRefundTransactionByOriginalOrderID(db.DB, orderID)
	}

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "退款订单未找到"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"original_order_id": refund.OriginalOrderID,
		"refund_to":         refund.RefundTo,
		"amount":            refund.Amount,
		"status":            refund.Status,
		"tx_signature":      refund.TXSignature,
		"block_height":      refund.BlockHeight,
		"created_at":        refund.CreatedAt,
	})
}
