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
		SerializedTx string `json:"serializedTx" binding:"required"` // base64 编码的序列化交易（用户已签名）
	}

	if err := c.ShouldBindJSON(&req); err != nil || req.SerializedTx == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":  "invalid request: serializedTx 字段是必需的",
			"detail": "请提供 base64 编码的已签名交易字符串。流程：前端用户签名交易 → 序列化为 base64 → 发送到后端",
		})
		return
	}

	signature, explorerURL, err := services.SignTx(c.Request.Context(), req.SerializedTx)
	if err != nil {
		// 提取错误详细信息
		errorMsg := err.Error()

		// 根据错误类型返回不同的状态码和消息
		switch {
		case err == services.ErrInvalidRequest:
			c.JSON(http.StatusBadRequest, gin.H{
				"error":  "invalid request",
				"detail": errorMsg,
			})
		case err == services.ErrBadTx:
			c.JSON(http.StatusBadRequest, gin.H{
				"error":  "bad tx",
				"detail": errorMsg,
			})
		case err == services.ErrPartialSignFailed:
			c.JSON(http.StatusBadRequest, gin.H{
				"error":  "partial sign failed",
				"detail": errorMsg,
			})
		case err == services.ErrSerializeFailed:
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  "serialize failed",
				"detail": errorMsg,
			})
		case err == services.ErrBroadcastFailed:
			c.JSON(http.StatusBadRequest, gin.H{
				"error":  "broadcast failed",
				"detail": errorMsg,
			})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  err.Error(),
				"detail": errorMsg,
			})
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
			// 返回详细的错误信息
			errorMsg := err.Error()
			c.JSON(http.StatusBadRequest, gin.H{
				"error":  "broadcast failed",
				"detail": errorMsg,
			})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  err.Error(),
				"detail": err.Error(),
			})
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
