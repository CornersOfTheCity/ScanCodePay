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
	// 注意：只检查，不在这里存储，等成功保存到数据库后再存储
	sigStr := sigInfo.Signature.String()
	if _, exists := l.processedSignatures.Load(sigStr); exists {
		return nil // 已处理过，跳过
	}

	return l.processSignature(sigInfo, address)
}

func (l *Listener) processSignature(sigInfo *rpc.TransactionSignature, address string) error {
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

	// 如果 BlockTime 为空，尝试从交易结果中获取
	if sigInfo.BlockTime == nil || *sigInfo.BlockTime == 0 {
		if tx.BlockTime != nil && *tx.BlockTime > 0 {
			sigInfo.BlockTime = tx.BlockTime
		} else {
			// 如果交易没有 BlockTime，可能是未确认的交易，跳过
			return nil
		}
	}

	if tx.Meta == nil {
		return nil // 静默跳过
	}

	// 第一步：先解析Memo（尽早过滤无效交易，避免不必要的USDC转账解析）
	var memo string
	if tx.Meta.LogMessages != nil {
		for _, logMsg := range tx.Meta.LogMessages {
			// 处理格式：Program logged: "Memo (len 8): "12345678""
			// 需要提取最后一个引号对内的内容（即实际的memo值）
			if strings.Contains(logMsg, "Memo (len") {
				// 查找 ": " 后面的引号（这是memo内容的开始）
				colonIdx := strings.Index(logMsg, `": "`)
				if colonIdx >= 0 {
					// 从 colonIdx+4 开始查找结束引号
					startIdx := colonIdx + 4
					endIdx := strings.LastIndex(logMsg, `"`)
					if endIdx > startIdx {
						memo = logMsg[startIdx:endIdx]
						memo = strings.TrimSpace(memo)
						if memo != "" {
							break
						}
					}
				}
				// 备选方案：如果上面没找到，尝试提取最后一个引号对
				if memo == "" {
					lastQuoteIdx := strings.LastIndex(logMsg, `"`)
					if lastQuoteIdx >= 0 {
						// 从后往前找前一个引号
						prevQuoteIdx := -1
						for i := lastQuoteIdx - 1; i >= 0; i-- {
							if logMsg[i] == '"' {
								prevQuoteIdx = i
								break
							}
						}
						if prevQuoteIdx >= 0 && prevQuoteIdx < lastQuoteIdx {
							memo = logMsg[prevQuoteIdx+1 : lastQuoteIdx]
							memo = strings.TrimSpace(memo)
							// 如果提取的内容包含 "Memo (len"，说明提取错了，继续查找
							if !strings.Contains(memo, "Memo (len") && memo != "" {
								break
							}
							memo = "" // 重置，继续查找
						}
					}
				}
			}
			// 处理格式：Program log: Memo "content" 或其他格式
			if memo == "" && (strings.Contains(logMsg, "Program log: Memo") || strings.Contains(logMsg, "Memo (")) {
				start := strings.Index(logMsg, "\"")
				end := strings.LastIndex(logMsg, "\"")
				if start >= 0 && end > start {
					memo = logMsg[start+1 : end]
					memo = strings.TrimSpace(memo)
					// 如果包含 "Memo (len"，说明这是格式描述，需要提取后面的内容
					if strings.Contains(memo, "Memo (len") {
						// 继续查找下一个引号对
						continue
					}
					break
				}
			}
			// 处理格式：Memo: content
			if memo == "" && strings.Contains(logMsg, "Memo:") {
				parts := strings.Split(logMsg, "Memo:")
				if len(parts) > 1 {
					memo = strings.TrimSpace(parts[1])
					// 如果包含引号，提取引号内的内容
					if strings.Contains(memo, "\"") {
						start := strings.Index(memo, "\"")
						end := strings.LastIndex(memo, "\"")
						if start >= 0 && end > start {
							memo = memo[start+1 : end]
							memo = strings.TrimSpace(memo)
						}
					}
					break
				}
			}
		}
	}

	// 第二步：根据memo判断交易类型，如果没有memo且不是退款格式，直接跳过（节省USDC转账解析）
	isRefund := memo != "" && strings.HasPrefix(memo, "refund:")

	// 调试日志：显示解析到的memo
	if memo != "" {
		log.Printf("[DEBUG] 交易 %s 解析到memo: %s", sigInfo.Signature.String(), memo)
	}

	if !isRefund && memo == "" {
		// 收款交易必须有memo（订单ID），没有memo的交易直接跳过，不解析USDC转账
		// 注意：这些可能是历史数据中的其他USDC交易（非收款/退款交易），或者是不相关的交易
		// 这是正常现象，不输出日志以减少噪音（如果需要调试，可以临时启用）
		// log.Printf("[DEBUG] 交易 %s 没有Memo且不是退款格式，跳过", sigInfo.Signature)
		return nil
	}

	// 第三步：有memo才解析USDC转账（优化：只解析有memo的交易）
	log.Printf("[DEBUG] 开始提取USDC转账，交易: %s, memo: %s", sigInfo.Signature.String(), memo)
	usdcTransfer, err := l.extractUSDCTransfer(tx, sigInfo.Signature)
	if err != nil || usdcTransfer == nil {
		// 如果有memo但提取USDC转账失败，记录日志以便调试
		if memo != "" {
			log.Printf("[WARN] 交易 %s 有memo (%s) 但无法提取USDC转账: %v", sigInfo.Signature.String(), memo, err)
		}
		return nil // 静默跳过
	}
	log.Printf("[DEBUG] USDC转账提取成功: 发送方=%s, 接收方=%s, 金额=%d",
		usdcTransfer.SourceOwner.String(), usdcTransfer.DestOwner.String(), usdcTransfer.Amount)

	// 第四步：根据memo判断交易类型并保存到对应表
	log.Printf("[DEBUG] 开始保存交易到数据库，交易: %s, 类型: %v", sigInfo.Signature.String(), isRefund)
	if err := l.saveTransactionByType(isRefund, memo, sigInfo, usdcTransfer); err != nil {
		log.Printf("[ERROR] 保存交易失败: %s, 错误: %v", sigInfo.Signature.String(), err)
		return err
	}
	return nil
}

// USDCTransfer USDC转账信息
type USDCTransfer struct {
	SourceOwner *solana.PublicKey // 发送方owner地址
	DestOwner   *solana.PublicKey // 接收方owner地址
	Amount      uint64            // 转账金额（lamports）
}

// extractUSDCTransfer 从TokenBalances提取USDC转账（不区分类型，提取所有USDC转账）
func (l *Listener) extractUSDCTransfer(tx *rpc.GetTransactionResult, signature solana.Signature) (*USDCTransfer, error) {
	// 查找所有USDC账户的余额变化
	// 找到余额增加的账户（接收方）和余额减少的账户（发送方）
	var sourceOwner, destOwner *solana.PublicKey
	var transferAmount uint64

	// 检查TokenBalances是否存在
	if tx.Meta == nil {
		return nil, fmt.Errorf("交易Meta为空")
	}

	log.Printf("[DEBUG] PreTokenBalances数量: %d, PostTokenBalances数量: %d",
		len(tx.Meta.PreTokenBalances), len(tx.Meta.PostTokenBalances))

	// 如果没有TokenBalances，尝试使用jsonParsed编码重新获取
	if len(tx.Meta.PreTokenBalances) == 0 && len(tx.Meta.PostTokenBalances) == 0 {
		log.Printf("[DEBUG] TokenBalances为空，尝试使用jsonParsed编码重新获取")
		// 尝试使用jsonParsed编码重新获取交易详情
		txParsed, err := l.client.GetTransaction(l.ctx, signature, &rpc.GetTransactionOpts{
			Encoding: solana.EncodingJSONParsed,
		})
		if err == nil && txParsed != nil && txParsed.Meta != nil {
			// 使用jsonParsed的结果
			tx.Meta = txParsed.Meta
			log.Printf("[DEBUG] 使用jsonParsed编码，PreTokenBalances数量: %d, PostTokenBalances数量: %d",
				len(tx.Meta.PreTokenBalances), len(tx.Meta.PostTokenBalances))
		} else {
			log.Printf("[DEBUG] jsonParsed编码获取失败: %v", err)
		}
	}

	// 如果TokenBalances中没有Owner信息，尝试使用jsonParsed编码重新获取
	hasOwnerInfo := false
	for _, post := range tx.Meta.PostTokenBalances {
		if post.Mint == l.mintPubkey && post.Owner != nil {
			hasOwnerInfo = true
			break
		}
	}

	if !hasOwnerInfo {
		// base64编码可能没有Owner信息，尝试使用jsonParsed编码
		txParsed, err := l.client.GetTransaction(l.ctx, signature, &rpc.GetTransactionOpts{
			Encoding: solana.EncodingJSONParsed,
		})
		if err == nil && txParsed != nil && txParsed.Meta != nil {
			// 使用jsonParsed的结果（包含Owner信息）
			tx.Meta = txParsed.Meta
		}
	}

	// 遍历所有PostTokenBalances，查找USDC余额增加的账户
	usdcFound := 0
	for _, post := range tx.Meta.PostTokenBalances {
		if post.Mint != l.mintPubkey {
			continue
		}
		usdcFound++

		// base64编码时Owner可能为nil，如果仍为nil，跳过（已经尝试过jsonParsed）
		if post.Owner == nil {
			log.Printf("[DEBUG] USDC账户索引 %v 的Owner为nil，跳过", post.AccountIndex)
			continue
		}
		log.Printf("[DEBUG] 检查USDC账户: Owner=%s, AccountIndex=%v", post.Owner.String(), post.AccountIndex)

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
		// 添加详细的错误信息以便调试
		var debugInfo string
		if len(tx.Meta.PostTokenBalances) == 0 {
			debugInfo = "PostTokenBalances为空"
		} else if len(tx.Meta.PreTokenBalances) == 0 {
			debugInfo = "PreTokenBalances为空"
		} else {
			usdcCount := 0
			for _, post := range tx.Meta.PostTokenBalances {
				if post.Mint == l.mintPubkey {
					usdcCount++
				}
			}
			debugInfo = fmt.Sprintf("找到%d个USDC账户，但无法确定转账关系", usdcCount)
		}
		return nil, fmt.Errorf("未找到有效的USDC转账: %s", debugInfo)
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

	// 注意：缓存检查在 processSignatureAsync 中已经完成，这里不需要再次检查
	// 如果到达这里，说明是新交易，需要处理

	// 检查数据库中是否已有此退款记录
	var existingRefund models.RefundTransaction
	err := l.db.Where("tx_signature = ?", txSignature).First(&existingRefund).Error
	if err == nil {
		log.Printf("[DEBUG] 退款交易 %s 已存在于数据库，跳过", txSignature)
		return nil // 静默跳过
	}
	if err != gorm.ErrRecordNotFound {
		log.Printf("[DEBUG] 查询退款交易时出错: %v", err)
		// 继续执行，不因为查询错误而跳过保存
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
		log.Printf("[ERROR] 保存退款交易失败: %s, 原订单: %s, 错误: %v", txSignature, originalOrderID, err)
		return err
	}
	log.Printf("[INFO] 成功处理退款交易: %s, 原订单: %s, 金额: %d",
		refundRecord.TXSignature, originalOrderID, transfer.Amount)
	// 成功保存后，将签名存入缓存，避免重复处理
	l.processedSignatures.Store(txSignature, true)
	return nil
}

// savePaymentTransaction 保存收款交易到数据库
func (l *Listener) savePaymentTransaction(memo string, sigInfo *rpc.TransactionSignature, transfer *USDCTransfer) error {
	orderID := strings.TrimSpace(memo)
	senderAddress := transfer.SourceOwner.String() // 付款人地址
	receiverAddress := transfer.DestOwner.String() // 收款人地址
	txSignature := sigInfo.Signature.String()

	// 注意：缓存检查在 processSignatureAsync 中已经完成，这里不需要再次检查
	// 如果到达这里，说明是新交易，需要处理

	// 检查数据库中是否已有此收款记录
	var existingTx models.Transaction
	err := l.db.Where("tx_signature = ? OR order_id = ?", txSignature, orderID).First(&existingTx).Error
	if err == nil {
		log.Printf("[DEBUG] 交易 %s 或订单 %s 已存在于数据库，跳过（已存在记录ID: %d）", txSignature, orderID, existingTx.ID)
		return nil // 静默跳过
	}
	if err != gorm.ErrRecordNotFound {
		log.Printf("[DEBUG] 查询数据库时出错: %v", err)
		// 继续执行，不因为查询错误而跳过保存
	}

	txRecord := &models.Transaction{
		OrderID:         orderID,
		ReceiverAddress: receiverAddress, // 收款人地址
		SenderAddress:   senderAddress,   // 付款人地址
		Amount:          transfer.Amount,
		TXSignature:     txSignature,
		BlockHeight:     uint64(sigInfo.Slot),
		Status:          "confirmed",
	}
	log.Printf("[DEBUG] 准备保存交易到数据库: 订单=%s, 发送方=%s, 接收方=%s, 金额=%d, 签名=%s",
		orderID, senderAddress, receiverAddress, transfer.Amount, txSignature)
	if err := db.SaveTransaction(l.db, txRecord); err != nil {
		log.Printf("[ERROR] 保存收款交易失败: %s, 订单: %s, 错误: %v", txSignature, orderID, err)
		return err
	}
	log.Printf("[INFO] 成功处理收款交易: %s, 订单: %s, 金额: %d, 记录ID: %d",
		txRecord.TXSignature, orderID, transfer.Amount, txRecord.ID)
	// 成功保存后，将签名存入缓存，避免重复处理
	l.processedSignatures.Store(txSignature, true)
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
