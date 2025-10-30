package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"ScanCodePay/internal/db"
)

func RegisterRoutes(r *gin.Engine) {
	r.GET("/transaction/:order_id", getTransaction)
}

func getTransaction(c *gin.Context) {
	orderID := c.Param("order_id")

	// 从 DB 查询（假设 DB 在全局或通过 context 传递，实际可使用 middleware 注入）
	var dbConn *gorm.DB // 在实际中从 context 或全局获取
	tx, err := db.GetTransactionByOrderID(dbConn, orderID)
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
