package handler

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"ScanCodePay/internal/db"
)

var (
	// startTime 记录服务启动时间
	startTime     time.Time
	startTimeOnce sync.Once
)

// InitStartTime 初始化服务启动时间（只执行一次）
func InitStartTime() {
	startTimeOnce.Do(func() {
		startTime = time.Now()
	})
}

// HealthzHandler 存活探针（liveness probe）
// 检查服务是否正在运行，总是返回 200（除非服务完全崩溃）
func HealthzHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"type":   "liveness",
	})
}

// ReadinessHandler 就绪探针（readiness probe）
// 检查服务是否准备好接收流量
// 启动等待30秒后才返回就绪状态
func ReadinessHandler(c *gin.Context) {
	// 检查启动时间是否已初始化
	if startTime.IsZero() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not ready",
			"type":    "readiness",
			"message": "服务启动时间未初始化",
		})
		return
	}

	// 检查是否已启动超过30秒
	elapsed := time.Since(startTime)
	if elapsed < 30*time.Second {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not ready",
			"type":    "readiness",
			"message": "服务启动中，等待就绪",
			"elapsed": elapsed.String(),
			"remaining": (30*time.Second - elapsed).String(),
		})
		return
	}

	// 检查数据库连接
	if db.DB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not ready",
			"type":    "readiness",
			"message": "数据库未初始化",
		})
		return
	}

	// 测试数据库连接
	sqlDB, err := db.DB.DB()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not ready",
			"type":    "readiness",
			"message": "无法获取数据库连接",
			"error":   err.Error(),
		})
		return
	}

	if err := sqlDB.Ping(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not ready",
			"type":    "readiness",
			"message": "数据库连接失败",
			"error":   err.Error(),
		})
		return
	}

	// 所有检查通过，服务就绪
	c.JSON(http.StatusOK, gin.H{
		"status":  "ready",
		"type":    "readiness",
		"message": "服务已就绪",
		"uptime":  elapsed.String(),
	})
}

