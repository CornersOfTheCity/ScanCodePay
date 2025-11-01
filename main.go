package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"ScanCodePay/internal/db"
	"ScanCodePay/internal/handler"
	"ScanCodePay/internal/models"
	"ScanCodePay/internal/services"
)

type Config struct {
	MySQL struct {
		Host     string `mapstructure:"host"`
		Port     int    `mapstructure:"port"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
		DBName   string `mapstructure:"dbname"`
	} `mapstructure:"mysql"`
	Solana struct {
		RPCURL       string `mapstructure:"rpc_url"`
		WSURL        string `mapstructure:"ws_url"` // 新增：WebSocket URL，例如 "wss://api.mainnet-beta.solana.com"
		USDC         string `mapstructure:"usdc_mint"`
		PayerSecret  string `mapstructure:"payer_secret"`
		RefundSecret string `mapstructure:"refund_secret"` // 退款账户私钥
	} `mapstructure:"solana"`
	App struct {
		PollInterval int `mapstructure:"poll_interval"` // 用于历史同步的间隔（秒），WebSocket 不需
		Port         int `mapstructure:"port"`
	} `mapstructure:"app"`
}

func main() {
	// 读取配置
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("读取配置失败:", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatal("解析配置失败:", err)
	}

	// 连接 MySQL 并初始化 DB
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.DBName)
	dbConn, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("MySQL 连接失败:", err)
	}

	// 设置全局 DB 变量
	db.DB = dbConn

	// 运行表结构迁移（创建新表或更新表结构）
	if err := dbConn.AutoMigrate(&models.Address{}, &models.Transaction{}, &models.RefundTransaction{}); err != nil {
		log.Fatal("表迁移失败:", err)
	}
	fmt.Println("数据库初始化完成")

	// 初始化 Solana 客户端与 Payer（从 config.yaml 读取配置）
	if err := services.InitSolana(); err != nil {
		log.Printf("初始化 Solana 客户端失败: %v", err)
	}

	// 预加载地址列表（静态加载）
	var addresses []models.Address
	if err := dbConn.Find(&addresses).Error; err != nil {
		log.Fatal("加载地址列表失败:", err)
	}
	if len(addresses) == 0 {
		log.Fatal("未找到任何地址，请先插入地址到数据库")
	}
	fmt.Printf("加载 %d 个地址\n", len(addresses))

	// 初始化监听器（在后台 goroutine 中运行）
	ctx, cancel := context.WithCancel(context.Background())
	go services.ListenerStart(ctx, dbConn, cfg.Solana.RPCURL, cfg.Solana.WSURL, cfg.Solana.USDC, time.Duration(cfg.App.PollInterval)*time.Second, addresses)

	// Gin 路由
	r := gin.Default()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	// 路由
	api := r.Group("/api")
	{
		// 交易查询
		api.GET("/transaction/:orderId", handler.GetTransactionHandler)

		// 签名账户地址查询
		api.GET("/getPayerAddress", handler.GetPayerAddressHandler)

		// 签名
		api.POST("/signTx", handler.SignTxHandler)

		// 退款接口（原 broadcastTx）
		api.POST("/refund", handler.RefundHandler)
	}

	// 启动服务器
	port := fmt.Sprintf(":%d", cfg.App.Port)
	fmt.Printf("服务器启动于端口 %s\n", port)
	if err := r.Run(port); err != nil {
		log.Fatal("Gin 服务器启动失败:", err)
	}

	// 优雅关闭（可选，实际中可添加 signal 处理）
	defer cancel()
}
