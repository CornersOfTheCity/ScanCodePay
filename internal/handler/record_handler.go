package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"ScanCodePay/internal/db"
)

func GetTransactionHandler(c *gin.Context) {
	orderID := c.Param("orderId")
	queryType := c.Query("type") // 查询类型: "payment" 或 "refund"，默认为 "payment"

	// 使用全局 DB 连接
	if db.DB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库未初始化"})
		return
	}

	// 根据类型查询
	if queryType == "refund" {
		// 查询退款订单
		refund, err := db.GetRefundTransactionByRefundOrderID(db.DB, orderID)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "退款订单未找到"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"refund_order_id":  refund.RefundOrderID,
			"original_order_id": refund.OriginalOrderID,
			"refund_to":        refund.RefundTo,
			"amount":           refund.Amount,
			"status":           refund.Status,
			"tx_signature":     refund.TXSignature,
			"block_height":     refund.BlockHeight,
			"created_at":       refund.CreatedAt,
		})
	} else {
		// 默认查询收款订单
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
			"type":         "payment",
		})
	}
}
