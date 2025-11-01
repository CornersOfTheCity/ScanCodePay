package services

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"gorm.io/gorm"

	"ScanCodePay/internal/db"
	"ScanCodePay/internal/models"
)

type Listener struct {
	client              *rpc.Client
	wsClient            *ws.Client
	mintPubkey          solana.PublicKey
	db                  *gorm.DB
	logsSub             *ws.LogSubscription // 日志订阅（使用mentions过滤器）
	tokenAccountToOwner map[string]string   // token账户(base58) -> owner地址(base58)
	mu                  sync.RWMutex
	ctx                 context.Context
	// 性能优化：并发控制和缓存
	processedSignatures sync.Map      // 已处理的交易签名缓存（快速去重）
	workerPool          chan struct{} // goroutine池，限制并发数
}

func ListenerStart(ctx context.Context, dbConn *gorm.DB, rpcURL, wsURL, usdcMint string, syncInterval time.Duration, configStartHeight uint64) {
	rpcClient := rpc.New(rpcURL)
	wsClient, err := ws.Connect(ctx, wsURL)
	if err != nil {
		log.Fatal("WebSocket 连接失败:", err)
	}
	defer wsClient.Close()

	mintPubkey := solana.MustPublicKeyFromBase58(usdcMint)
	payerPubkey := Payer.PublicKey()

	log.Printf("监控代付gasfee地址签名的所有交易: %s", payerPubkey.String())

	l := &Listener{
		client:              rpcClient,
		wsClient:            wsClient,
		mintPubkey:          mintPubkey,
		db:                  dbConn,
		tokenAccountToOwner: make(map[string]string),
		ctx:                 ctx,
		workerPool:          make(chan struct{}, 20), // 限制并发数为20
	}

	// 1. 获取数据库中的最高高度
	dbMaxHeight, err := l.getMaxBlockHeight()
	if err != nil {
		log.Printf("获取数据库最高高度失败: %v，使用配置的起始高度", err)
		dbMaxHeight = 0
	}

	// 2. 比较配置的起始高度和数据库最高高度，取较高的作为同步起点
	syncStartHeight := configStartHeight
	if dbMaxHeight > syncStartHeight {
		syncStartHeight = dbMaxHeight
		log.Printf("数据库最高高度 %d 大于配置起始高度 %d，使用数据库高度作为同步起点", dbMaxHeight, configStartHeight)
	} else if configStartHeight > dbMaxHeight {
		log.Printf("配置起始高度 %d 大于数据库最高高度 %d，使用配置高度作为同步起点", configStartHeight, dbMaxHeight)
	} else {
		log.Printf("同步起始高度: %d (配置: %d, 数据库: %d)", syncStartHeight, configStartHeight, dbMaxHeight)
	}

	// 3. 获取当前槽位（用于订阅和同步终点）
	currentSlot, err := l.client.GetSlot(ctx, rpc.CommitmentFinalized)
	if err != nil {
		log.Fatal("获取当前槽位失败:", err)
	}
	currentSlotUint := uint64(currentSlot)

	// 4. 先启动 WebSocket 实时监听（立即开始捕获新交易，避免遗漏）
	log.Println("启动 WebSocket 实时监听...")
	if err := l.subscribeLogs(payerPubkey); err != nil {
		log.Printf("订阅失败: %v", err)
	} else {
		log.Println("WebSocket 实时监听已启动，开始捕获新交易")
	}

	// 5. 判断是否需要历史同步，如果需要则在后台并发执行
	// 如果数据库为空且配置的起始高度为0，则只开启订阅，不进行历史同步
	if dbMaxHeight == 0 && configStartHeight == 0 {
		log.Println("数据库为空且配置起始高度为0，仅开启订阅，不进行历史同步")
	} else if syncStartHeight < currentSlotUint {
		// 需要同步历史记录：从同步起点到订阅启动时的当前高度
		// 在后台goroutine中执行，与订阅并发进行
		log.Printf("在后台启动历史同步: 从槽位 %d 到槽位 %d（与订阅并发执行）", syncStartHeight, currentSlotUint)

		// 记录订阅启动时的槽位，历史同步到这个槽位
		subscriptionStartSlot := currentSlotUint

		go func() {
			var lastProcessedSlot uint64 = syncStartHeight
			if err := l.syncHistoryFromSlot(payerPubkey.String(), syncStartHeight, subscriptionStartSlot, syncInterval, &lastProcessedSlot); err != nil {
				log.Printf("历史同步失败: %v", err)
				log.Println("警告: 历史同步未完成，可能存在遗漏的交易")
			} else {
				log.Printf("历史同步完成: 从槽位 %d 到槽位 %d", syncStartHeight, lastProcessedSlot)
			}
		}()
	} else {
		log.Printf("同步起始高度 %d 已大于等于当前槽位 %d，无需历史同步", syncStartHeight, currentSlotUint)
	}

	// 保持运行
	<-ctx.Done()
	l.unsubscribeLogs()
	log.Println("监听器停止")
}

// getMaxBlockHeight 获取数据库中最大的block_height
func (l *Listener) getMaxBlockHeight() (uint64, error) {
	return db.GetMaxBlockHeight(l.db)
}

// syncHistoryFromSlot 从指定槽位同步历史交易到目标槽位
func (l *Listener) syncHistoryFromSlot(address string, startSlot, endSlot uint64, interval time.Duration, lastProcessedSlot *uint64) error {
	addressPubkey := solana.MustPublicKeyFromBase58(address)
	log.Printf("地址 %s: 从槽位 %d 同步到槽位 %d", address, startSlot, endSlot)

	limit := 100
	var before solana.Signature
	hasBefore := false
	processedCount := 0

	for {
		opts := &rpc.GetSignaturesForAddressOpts{
			Limit: &limit,
		}
		if hasBefore {
			opts.Before = before
		}

		signatures, err := l.client.GetSignaturesForAddressWithOpts(l.ctx, addressPubkey, opts)
		if err != nil || len(signatures) == 0 {
			break
		}

		// 过滤：只处理槽位在 [startSlot, endSlot] 范围内的交易
		// GetSignaturesForAddress 返回的交易是从新到旧排序的
		shouldBreak := false
		var wg sync.WaitGroup

		for i := range signatures {
			sigInfo := signatures[i]
			slot := uint64(sigInfo.Slot)

			// 如果槽位大于结束槽位，跳过（这些是未来的交易，可能还没确认）
			if slot > endSlot {
				continue
			}

			// 如果槽位小于起始槽位，说明已经超出了同步范围，可以停止
			if slot < startSlot {
				shouldBreak = true
				break
			}

			// 槽位在 [startSlot, endSlot] 范围内，处理这个交易
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				sig := signatures[idx]
				if err := l.processSignatureAsync(sig, address); err == nil {
					processedCount++
					if uint64(sig.Slot) > *lastProcessedSlot {
						*lastProcessedSlot = uint64(sig.Slot)
					}
				}
			}(i)
		}
		wg.Wait()

		// 如果已经遇到小于startSlot的交易，说明同步完成
		if shouldBreak {
			break
		}

		before = signatures[len(signatures)-1].Signature
		hasBefore = true
		time.Sleep(interval)
	}

	log.Printf("历史同步完成，处理了 %d 个交易", processedCount)
	if lastProcessedSlot != nil && *lastProcessedSlot > 0 {
		log.Printf("最后处理的槽位: %d", *lastProcessedSlot)
	}
	return nil
}

// subscribeLogs 使用logsSubscribe + mentions过滤器订阅包含Payer地址的交易日志
func (l *Listener) subscribeLogs(payerPubkey solana.PublicKey) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 使用mentions过滤器：监听包含Payer地址的所有交易
	// 因为fee payer总是交易的第一个账户，会被"提及"，所以可以捕获所有相关交易
	// 这种方法比AccountSubscribe更高效，因为：
	// 1. 直接订阅日志，无需轮询账户变化
	// 2. 实时捕获所有包含该地址的交易
	// 3. 减少RPC调用（只在收到通知时获取交易详情）
	sub, err := l.wsClient.LogsSubscribeMentions(payerPubkey, rpc.CommitmentFinalized)
	if err != nil {
		return fmt.Errorf("订阅日志失败: %w", err)
	}
	l.logsSub = sub

	go l.handleLogsNotifications()
	log.Printf("订阅包含地址 %s 的交易日志（使用mentions过滤器）", payerPubkey.String())
	return nil
}

func (l *Listener) handleLogsNotifications() {
	maxRetries := 5
	retryCount := 0
	payerPubkey := Payer.PublicKey()

	for retryCount < maxRetries {
		result, err := l.logsSub.Recv(l.ctx)
		if err != nil {
			log.Printf("日志通知接收失败: %v，重连中 (第 %d/%d 次)...", err, retryCount+1, maxRetries)
			if l.logsSub != nil {
				l.logsSub.Unsubscribe()
			}
			time.Sleep(time.Duration(retryCount+1) * 5 * time.Second) // 递增重试间隔

			// 重新订阅
			l.mu.Lock()
			newSub, err := l.wsClient.LogsSubscribeMentions(payerPubkey, rpc.CommitmentFinalized)
			if err != nil {
				l.mu.Unlock()
				retryCount++
				if retryCount >= maxRetries {
					log.Printf("日志订阅重连失败，已达最大重试次数")
					return
				}
				continue
			}
			l.logsSub = newSub
			l.mu.Unlock()
			retryCount = 0 // 重置重试计数
			log.Printf("日志订阅重连成功")
			continue
		}

		// 成功接收，重置重试计数
		retryCount = 0

		// 从日志通知中提取交易签名
		if result == nil {
			continue
		}

		signature := result.Value.Signature
		if signature.IsZero() {
			continue
		}

		log.Printf("[DEBUG] 收到交易日志通知: %s", signature.String())

		// 获取交易详情并处理
		// 注意：logs通知可能不包含BlockTime，需要通过GetTransaction获取
		var blockTime *solana.UnixTimeSeconds
		sigInfo := &rpc.TransactionSignature{
			Signature: signature,
			Slot:      result.Context.Slot,
			BlockTime: blockTime, // 将从GetTransaction中获取
		}

		// 并发处理交易（使用goroutine池）
		go l.processSignatureAsync(sigInfo, payerPubkey.String())
	}
}

// processSignatureAsync 异步处理交易（使用goroutine池限制并发）
func (l *Listener) processSignatureAsync(sigInfo *rpc.TransactionSignature, address string) error {
	// 限制并发数
	l.workerPool <- struct{}{}
	defer func() { <-l.workerPool }()

	// 快速去重检查（使用内存缓存）
	sigStr := sigInfo.Signature.String()
	if _, exists := l.processedSignatures.LoadOrStore(sigStr, true); exists {
		return nil // 已处理过，跳过
	}

	return l.processSignature(sigInfo, address)
}

func (l *Listener) processSignature(sigInfo *rpc.TransactionSignature, address string) error {
	if sigInfo.BlockTime == nil || *sigInfo.BlockTime == 0 {
		return nil // 未确认，静默跳过（性能优化：减少日志）
	}

	// 获取交易详情，优先使用jsonParsed编码以便解析SPL Token指令
	// 注意：某些交易可能因为版本问题无法用jsonParsed解析，需要回退到base64
	var tx *rpc.GetTransactionResult
	var err error

	// 性能优化：直接使用base64编码（更快，避免两次RPC调用）
	// 如果需要Owner信息，base64编码时可能为空，但可以通过TokenBalances计算得出
	tx, err = l.client.GetTransaction(l.ctx, sigInfo.Signature, &rpc.GetTransactionOpts{
		Encoding: solana.EncodingBase64,
	})
	if err != nil || tx == nil {
		return err // 静默失败（性能优化）
	}

	if tx.Meta == nil {
		return nil // 静默跳过
	}

	// 第一步：先解析Memo（尽早过滤无效交易，避免不必要的USDC转账解析）
	var memo string
	if tx.Meta.LogMessages != nil {
		for _, logMsg := range tx.Meta.LogMessages {
			if strings.Contains(logMsg, "Program log: Memo") || strings.Contains(logMsg, "Memo (") {
				start := strings.Index(logMsg, "\"")
				end := strings.LastIndex(logMsg, "\"")
				if start >= 0 && end > start {
					memo = logMsg[start+1 : end]
					memo = strings.TrimSpace(memo)
					break
				}
			}
			if memo == "" && strings.Contains(logMsg, "Memo:") {
				parts := strings.Split(logMsg, "Memo:")
				if len(parts) > 1 {
					memo = strings.TrimSpace(parts[1])
					break
				}
			}
		}
	}

	// 第二步：根据memo判断交易类型，如果没有memo且不是退款格式，直接跳过（节省USDC转账解析）
	isRefund := memo != "" && strings.HasPrefix(memo, "refund:")

	if !isRefund && memo == "" {
		// 收款交易必须有memo（订单ID），没有memo的交易直接跳过，不解析USDC转账
		// 注意：这些可能是历史数据中的其他USDC交易（非收款/退款交易），或者是不相关的交易
		// 这是正常现象，不输出日志以减少噪音（如果需要调试，可以临时启用）
		// log.Printf("[DEBUG] 交易 %s 没有Memo且不是退款格式，跳过", sigInfo.Signature)
		return nil
	}

	// 第三步：有memo才解析USDC转账（优化：只解析有memo的交易）
	usdcTransfer, err := l.extractUSDCTransfer(tx)
	if err != nil || usdcTransfer == nil {
		return nil // 静默跳过
	}

	// 第四步：根据memo判断交易类型并保存到对应表
	return l.saveTransactionByType(isRefund, memo, sigInfo, usdcTransfer)
}

// USDCTransfer USDC转账信息
type USDCTransfer struct {
	SourceOwner *solana.PublicKey // 发送方owner地址
	DestOwner   *solana.PublicKey // 接收方owner地址
	Amount      uint64            // 转账金额（lamports）
}

// extractUSDCTransfer 从TokenBalances提取USDC转账（不区分类型，提取所有USDC转账）
func (l *Listener) extractUSDCTransfer(tx *rpc.GetTransactionResult) (*USDCTransfer, error) {
	// 查找所有USDC账户的余额变化
	// 找到余额增加的账户（接收方）和余额减少的账户（发送方）
	var sourceOwner, destOwner *solana.PublicKey
	var transferAmount uint64

	// 遍历所有PostTokenBalances，查找USDC余额增加的账户
	// 性能优化：base64编码时Owner可能为nil，需要通过AccountIndex从交易中获取
	for _, post := range tx.Meta.PostTokenBalances {
		if post.Mint != l.mintPubkey {
			continue
		}
		// base64编码时Owner可能为nil，需要通过其他方式获取
		if post.Owner == nil {
			// base64编码时无法直接获取Owner，跳过（需要额外查询）
			continue
		}

		// 找到对应的PreTokenBalance
		for _, pre := range tx.Meta.PreTokenBalances {
			if pre.AccountIndex == post.AccountIndex && pre.Mint == l.mintPubkey {
				var preAmount, postAmount uint64

				if pre.UiTokenAmount != nil && pre.UiTokenAmount.UiAmount != nil {
					decimals := uint64(pre.UiTokenAmount.Decimals)
					if decimals == 0 {
						decimals = 6
					}
					preAmount = uint64(*pre.UiTokenAmount.UiAmount * float64(1e6))
				}

				if post.UiTokenAmount != nil && post.UiTokenAmount.UiAmount != nil {
					decimals := uint64(post.UiTokenAmount.Decimals)
					if decimals == 0 {
						decimals = 6
					}
					postAmount = uint64(*post.UiTokenAmount.UiAmount * float64(1e6))
				}

				// 如果余额增加，说明是接收方
				if postAmount > preAmount {
					destOwner = post.Owner
					transferAmount = postAmount - preAmount

					// 查找发送方（余额减少的账户）
					for _, pre2 := range tx.Meta.PreTokenBalances {
						if pre2.Mint != l.mintPubkey || pre2.Owner == nil {
							continue
						}
						for _, post2 := range tx.Meta.PostTokenBalances {
							if post2.AccountIndex == pre2.AccountIndex && post2.Mint == l.mintPubkey {
								var preAmt, postAmt uint64
								if pre2.UiTokenAmount != nil && pre2.UiTokenAmount.UiAmount != nil {
									decimals := uint64(pre2.UiTokenAmount.Decimals)
									if decimals == 0 {
										decimals = 6
									}
									preAmt = uint64(*pre2.UiTokenAmount.UiAmount * float64(1e6))
								}
								if post2.UiTokenAmount != nil && post2.UiTokenAmount.UiAmount != nil {
									decimals := uint64(post2.UiTokenAmount.Decimals)
									if decimals == 0 {
										decimals = 6
									}
									postAmt = uint64(*post2.UiTokenAmount.UiAmount * float64(1e6))
								}
								if preAmt > postAmt {
									sourceOwner = pre2.Owner
									break
								}
							}
						}
						if sourceOwner != nil {
							break
						}
					}
					break
				}
			}
		}
		if destOwner != nil && sourceOwner != nil {
			break
		}
	}

	if transferAmount == 0 || sourceOwner == nil || destOwner == nil {
		return nil, fmt.Errorf("未找到有效的USDC转账")
	}

	return &USDCTransfer{
		SourceOwner: sourceOwner,
		DestOwner:   destOwner,
		Amount:      transferAmount,
	}, nil
}

// saveTransactionByType 根据交易类型保存到对应表
func (l *Listener) saveTransactionByType(isRefund bool, memo string, sigInfo *rpc.TransactionSignature, transfer *USDCTransfer) error {
	if isRefund {
		return l.saveRefundTransaction(memo, sigInfo, transfer)
	}
	return l.savePaymentTransaction(memo, sigInfo, transfer)
}

// saveRefundTransaction 保存退款交易到数据库
func (l *Listener) saveRefundTransaction(memo string, sigInfo *rpc.TransactionSignature, transfer *USDCTransfer) error {
	refundToAddress := transfer.DestOwner.String()
	txSignature := sigInfo.Signature.String()

	// 性能优化：先检查缓存，再查数据库
	if _, exists := l.processedSignatures.Load(txSignature); exists {
		return nil // 已处理过
	}

	// 检查数据库中是否已有此退款记录
	var existingRefund models.RefundTransaction
	if err := l.db.Where("tx_signature = ?", txSignature).First(&existingRefund).Error; err == nil {
		return nil // 静默跳过
	}

	// 从memo中提取原订单ID
	originalOrderID := ""
	if memo != "" {
		if strings.HasPrefix(memo, "refund:") {
			originalOrderID = strings.TrimPrefix(memo, "refund:")
			originalOrderID = strings.TrimSpace(originalOrderID)
		} else if strings.HasPrefix(memo, "refund-") {
			// 兼容旧格式: refund-timestamp-originalOrderID
			parts := strings.Split(strings.TrimSpace(memo), "-")
			if len(parts) >= 3 {
				originalOrderID = strings.Join(parts[2:], "-")
			}
		}
	}

	refundRecord := &models.RefundTransaction{
		OriginalOrderID: originalOrderID,
		RefundTo:        refundToAddress,
		Amount:          transfer.Amount,
		TXSignature:     txSignature,
		BlockHeight:     uint64(sigInfo.Slot),
		Status:          "confirmed",
	}
	if err := db.SaveRefundTransaction(l.db, refundRecord); err != nil {
		return err
	}
	// 性能优化：只在关键成功时输出日志（可选）
	// log.Printf("处理退款交易 %s: 原订单ID %s", refundRecord.TXSignature, originalOrderID)
	return nil
}

// savePaymentTransaction 保存收款交易到数据库
func (l *Listener) savePaymentTransaction(memo string, sigInfo *rpc.TransactionSignature, transfer *USDCTransfer) error {
	orderID := strings.TrimSpace(memo)
	payerAddress := transfer.SourceOwner.String()
	txSignature := sigInfo.Signature.String()

	// 性能优化：先检查缓存，再查数据库
	if _, exists := l.processedSignatures.Load(txSignature); exists {
		return nil // 已处理过
	}

	// 检查数据库中是否已有此收款记录
	var existingTx models.Transaction
	if err := l.db.Where("tx_signature = ? OR order_id = ?", txSignature, orderID).First(&existingTx).Error; err == nil {
		return nil // 静默跳过
	}

	txRecord := &models.Transaction{
		OrderID:     orderID,
		Address:     payerAddress,
		Amount:      transfer.Amount,
		TXSignature: txSignature,
		BlockHeight: uint64(sigInfo.Slot),
		Status:      "confirmed",
	}
	if err := db.SaveTransaction(l.db, txRecord); err != nil {
		return err
	}
	// 性能优化：只在关键成功时输出日志（可选）
	// log.Printf("处理收款交易 %s: 订单 %s", txRecord.TXSignature, orderID)
	return nil
}

func (l *Listener) unsubscribeLogs() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.logsSub != nil {
		l.logsSub.Unsubscribe()
		l.logsSub = nil
	}
}
