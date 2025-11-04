package handler

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"ScanCodePay/internal/db"
	"ScanCodePay/internal/models"
	"ScanCodePay/internal/services"
	"ScanCodePay/utils"
)

// refundRequestCache 防重复请求缓存
type refundRequestCache struct {
	mu      sync.RWMutex
	entries map[string]time.Time
}

var refundCache = &refundRequestCache{
	entries: make(map[string]time.Time),
}

// generateRefundKey 生成退款请求的唯一键（orderID + refundTo + amount）
func generateRefundKey(orderID, refundTo string, amount uint64) string {
	return fmt.Sprintf("%s:%s:%d", orderID, refundTo, amount)
}

// checkAndSet 检查请求是否在10秒内重复，如果不是则设置
// 返回 true 表示重复请求，false 表示新请求
func (c *refundRequestCache) checkAndSet(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 清理过期条目（超过10秒）
	now := time.Now()
	for k, t := range c.entries {
		if now.Sub(t) > 10*time.Second {
			delete(c.entries, k)
		}
	}

	// 检查是否存在
	if lastTime, exists := c.entries[key]; exists {
		if now.Sub(lastTime) <= 10*time.Second {
			return true // 重复请求
		}
		// 超过10秒，更新时间为当前时间
		c.entries[key] = now
		return false
	}

	// 新请求，记录时间
	c.entries[key] = now
	return false
}

// clear 清除指定键（退款成功后可以清除，但也可以等自动过期）
func (c *refundRequestCache) clear(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

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

// RefundHandler 退款接口
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

	// 防重复请求检查：同一笔退款（orderID + refundTo + amount）在10秒内只能调用一次
	refundKey := generateRefundKey(req.OrderID, req.RefundTo, req.Amount)
	if refundCache.checkAndSet(refundKey) {
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error":  "重复请求",
			"detail": "同一笔退款在10秒内只能调用一次，请稍后再试",
		})
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

	// 查询该订单的所有退款记录
	existingRefunds, err := db.GetRefundTransactionsByOriginalOrderID(db.DB, req.OrderID)
	if err != nil && err != gorm.ErrRecordNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询退款记录失败"})
		return
	}

	// 计算已退款总金额
	// 注意：同时计算 "confirmed" 和 "pending" 状态的退款，避免并发退款导致超额
	var totalRefundedAmount uint64 = 0
	for _, refund := range existingRefunds {
		// 计算已确认和待处理的退款（排除失败的退款）
		if refund.Status == "confirmed" || refund.Status == "pending" {
			totalRefundedAmount += refund.Amount
		}
	}

	// 验证：已退款总金额 + 本次退款金额不能大于原订单金额
	if totalRefundedAmount+req.Amount > originalTx.Amount {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "退款金额超出限制",
			"detail": map[string]interface{}{
				"originalAmount":  originalTx.Amount,
				"totalRefunded":   totalRefundedAmount,
				"currentRefund":   req.Amount,
				"remainingAmount": originalTx.Amount - totalRefundedAmount,
				"exceededAmount":  (totalRefundedAmount + req.Amount) - originalTx.Amount,
			},
		})
		return
	}

	// 验证单次退款金额不能大于收款金额（保留原有验证，但允许多次退款）
	if req.Amount > originalTx.Amount {
		c.JSON(http.StatusBadRequest, gin.H{"error": "单次退款金额不能大于收款金额"})
		return
	}

	// 创建退款交易并广播
	signature, explorerURL, err := services.CreateRefundTx(c.Request.Context(), req.OrderID, req.RefundTo, req.Amount)
	if err != nil {
		// 退款失败时，清除缓存，允许重试
		refundCache.clear(refundKey)

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
		// 保存失败时，清除缓存，允许重试
		refundCache.clear(refundKey)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存退款记录失败"})
		return
	}

	// 退款成功，清除缓存（允许后续可能的退款）
	// 注意：也可以不清除，让缓存自动过期（10秒后）
	// 这里选择清除，以便如果退款失败，用户可以立即重试
	refundCache.clear(refundKey)

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

// GetVersionHandler 返回服务版本号
func GetVersionHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"version": utils.Version,
	})
}
