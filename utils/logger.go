package utils

import "log"

// Logger 简单日志封装
type Logger struct{}

// Info 信息日志
func (l *Logger) Info(msg string, args ...interface{}) {
	log.Printf("[INFO] "+msg, args...)
}

// Warn 警告日志
func (l *Logger) Warn(msg string, args ...interface{}) {
	log.Printf("[WARN] "+msg, args...)
}

// Error 错误日志
func (l *Logger) Error(msg string, args ...interface{}) {
	log.Printf("[ERROR] "+msg, args...)
}

var DefaultLogger = &Logger{}
