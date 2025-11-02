package middleware

import (
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
)

// LocalOnly 中间件：只允许本地访问（127.0.0.1 或 ::1）
func LocalOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取客户端 IP
		clientIP := c.ClientIP()
		
		// 检查是否为本地 IP
		ip := net.ParseIP(clientIP)
		if ip == nil {
			c.JSON(http.StatusForbidden, gin.H{"error": "禁止访问"})
			c.Abort()
			return
		}

		// 允许的本地 IP：127.0.0.1 和 ::1
		isLocal := ip.IsLoopback()
		
		if !isLocal {
			c.JSON(http.StatusForbidden, gin.H{"error": "禁止访问：仅允许本地访问"})
			c.Abort()
			return
		}

		c.Next()
	}
}

