package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"ScanCodePay/internal/db"
	"ScanCodePay/internal/models"
	"ScanCodePay/internal/services"
)

func SignTxHandler(c *gin.Context) {
	var req struct {
		SerializedTx string `json:"serializedTx"`
	}

	if err := c.ShouldBindJSON(&req); err != nil || req.SerializedTx == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	signature, explorerURL, err := services.SignTx(c.Request.Context(), req.SerializedTx)
	if err != nil {
		switch err {
		case services.ErrInvalidRequest:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		case services.ErrBadTx:
			c.JSON(http.StatusBadRequest, gin.H{"error": "bad tx"})
		case services.ErrPartialSignFailed:
			c.JSON(http.StatusBadRequest, gin.H{"error": "partial sign failed"})
		case services.ErrSerializeFailed:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "serialize failed"})
		case services.ErrBroadcastFailed:
			c.JSON(http.StatusBadRequest, gin.H{"error": "broadcast failed"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"signature":   signature,
		"explorerUrl": explorerURL,
	})
}

// RefundHandler 退款接口（原 broadcastTx 接口）
func RefundHandler(c *gin.Context) {
	var req struct {
		OrderID  string `json:"orderId" binding:"required"`  // 原收款订单ID
		RefundTo string `json:"refundTo" binding:"required"` // 退款收款人地址
		Amount   uint64 `json:"amount" binding:"required"`   // 退款金额（lamports）
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	// 使用全局 DB 连接
	if db.DB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库未初始化"})
		return
	}

	// 查询原收款订单
	originalTx, err := db.GetTransactionByOrderID(db.DB, req.OrderID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "订单未找到"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	// 验证退款金额不能大于收款金额
	if req.Amount > originalTx.Amount {
		c.JSON(http.StatusBadRequest, gin.H{"error": "退款金额不能大于收款金额"})
		return
	}

	// 创建退款交易并广播
	signature, explorerURL, err := services.CreateRefundTx(c.Request.Context(), req.OrderID, req.RefundTo, req.Amount)
	if err != nil {
		switch err {
		case services.ErrInvalidRequest:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		case services.ErrPartialSignFailed:
			c.JSON(http.StatusBadRequest, gin.H{"error": "partial sign failed"})
		case services.ErrSerializeFailed:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "serialize failed"})
		case services.ErrBroadcastFailed:
			c.JSON(http.StatusBadRequest, gin.H{"error": "broadcast failed"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	// 保存退款记录
	refundRecord := &models.RefundTransaction{
		OriginalOrderID: req.OrderID,
		RefundTo:        req.RefundTo,
		Amount:          req.Amount,
		TXSignature:     signature,
		Status:          "pending",
	}

	if err := db.SaveRefundTransaction(db.DB, refundRecord); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存退款记录失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"originalOrderID": req.OrderID,
		"signature":       signature,
		"explorerUrl":     explorerURL,
	})
}

// GetPayerAddressHandler 返回用于签名的账户地址
func GetPayerAddressHandler(c *gin.Context) {
	address := services.GetPayerAddress()
	if address == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "签名账户未初始化"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"address": address,
	})
}
