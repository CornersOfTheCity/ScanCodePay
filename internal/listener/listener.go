package listener

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
)

type Listener struct {
	client              *rpc.Client
	wsClient            *ws.Client
	mintPubkey          solana.PublicKey
	db                  *gorm.DB
	subs                map[string]*ws.AccountSubscription // 订阅（以token账户为键）
	tokenAccountToOwner map[string]string                  // token账户(base58) -> owner地址(base58)
	mu                  sync.RWMutex
	ctx                 context.Context
}

func Start(ctx context.Context, dbConn *gorm.DB, rpcURL, wsURL, usdcMint string, syncInterval time.Duration, addresses []db.Address) {
	rpcClient := rpc.New(rpcURL)
	wsClient, err := ws.Connect(ctx, wsURL)
	if err != nil {
		log.Fatal("WebSocket 连接失败:", err)
	}
	defer wsClient.Close()

	mintPubkey := solana.MustPublicKeyFromBase58(usdcMint)

	l := &Listener{
		client:              rpcClient,
		wsClient:            wsClient,
		mintPubkey:          mintPubkey,
		db:                  dbConn,
		subs:                make(map[string]*ws.AccountSubscription),
		tokenAccountToOwner: make(map[string]string),
		ctx:                 ctx,
	}

	log.Println("开始历史同步...")
	var historyEndSlot uint64 = 0
	if err := l.syncHistory(addresses, syncInterval); err != nil {
		log.Printf("历史同步失败: %v", err)
		// 历史同步失败时，仍然启动实时监听，但记录警告
		log.Println("警告: 历史同步未完成，可能存在遗漏的交易")
	} else {
		log.Println("历史同步完成")
		// 获取历史同步完成时的最新槽位
		currentSlot, err := l.client.GetSlot(l.ctx, rpc.CommitmentFinalized)
		if err == nil {
			historyEndSlot = uint64(currentSlot)
		}
	}

	// 先启动 WebSocket 实时监听（立即开始监听新的区块变化）
	log.Println("启动 WebSocket 实时监听...")
	if err := l.subscribeAll(addresses); err != nil {
		log.Printf("订阅失败: %v", err)
	}

	// 在 WebSocket 启动后，再同步空白期（历史同步期间产生的新交易）
	// 这样可以确保 WebSocket 已经开始监听，同步期间产生的新交易不会被遗漏
	if historyEndSlot > 0 {
		log.Println("同步历史空白期，确保无遗漏...")
		if err := l.syncHistoryGap(addresses, historyEndSlot, syncInterval); err != nil {
			log.Printf("空白期同步失败: %v", err)
		}
		log.Println("同步历史空白期完成")
	}

	// 保持运行，监听 ctx 取消
	<-ctx.Done()
	l.unsubscribeAll()
	log.Println("监听器停止")
}

func (l *Listener) syncHistory(addresses []db.Address, interval time.Duration) error {
	successCount := 0
	for _, addr := range addresses {
		var latestAddr db.Address
		if err := l.db.Where("address = ?", addr.Address).First(&latestAddr).Error; err != nil {
			log.Printf("地址 %s 数据库查询失败: %v，跳过", addr.Address, err)
			continue // 跳过失败的地址，继续处理其他地址
		}
		scanHeight := latestAddr.ScanHeight

		// 获取当前最新槽位
		currentSlot, err := l.client.GetSlot(l.ctx, rpc.CommitmentFinalized)
		if err != nil {
			return err
		}
		log.Printf("地址 %s: 从槽 %d 同步到 %d", addr.Address, scanHeight, currentSlot)

		// 选择用于拉取签名的账户：优先使用 USDC Token Account，其次使用 owner 本身
		pullPubkey := solana.MustPublicKeyFromBase58(addr.Address)
		if toks, err := l.getUSDCAccountsForOwner(addr.Address); err == nil && len(toks) > 0 {
			pullPubkey = toks[0]
		}

		// 分批轮询历史签名（从新到旧，使用 Before 签名进行分页）
		var before solana.Signature
		hasBefore := false
		limit := 500
		for {
			opts := &rpc.GetSignaturesForAddressOpts{
				Limit: &limit, // 最大 1000，但保守
			}
			if hasBefore {
				opts.Before = before
			}
			signatures, err := l.client.GetSignaturesForAddressWithOpts(
				l.ctx,
				pullPubkey,
				opts,
			)
			if err != nil {
				log.Printf("历史同步 %s 失败: %v", addr.Address, err)
				time.Sleep(interval)
				continue
			}

			if len(signatures) == 0 {
				log.Printf("[DEBUG] 地址 %s 没有更多签名", addr.Address)
				break
			}

			log.Printf("[DEBUG] 地址 %s 获取到 %d 个签名，开始处理", addr.Address, len(signatures))

			// 处理签名（signatures 是从新到旧）
			processedCount := 0
			skippedCount := 0
			allOld := true // 标记是否所有交易都是旧的（槽位 <= scanHeight）

			for _, sigInfo := range signatures {
				slot := uint64(sigInfo.Slot)
				if slot <= scanHeight {
					log.Printf("[DEBUG] 签名 %s 槽位 %d <= scanHeight %d，跳过", sigInfo.Signature, slot, scanHeight)
					skippedCount++
					continue // 跳过旧交易，继续处理后面的
				}

				allOld = false // 找到新交易
				log.Printf("[DEBUG] 准备处理签名 %s (槽位: %d, 时间: %v)", sigInfo.Signature, slot, sigInfo.BlockTime)
				if err := l.processSignature(sigInfo, addr.Address); err != nil {
					log.Printf("[ERROR] 处理历史交易 %s 失败: %v", sigInfo.Signature, err)
				} else {
					processedCount++
				}
			}

			log.Printf("[DEBUG] 本批处理: 处理了 %d 个交易，跳过了 %d 个旧交易", processedCount, skippedCount)

			// 检查这批中最小的槽位
			minSlotInBatch := uint64(0)
			if len(signatures) > 0 {
				minSlotInBatch = uint64(signatures[len(signatures)-1].Slot) // 最后一个是最旧的
				log.Printf("[DEBUG] 本批最小槽位: %d, scanHeight: %d", minSlotInBatch, scanHeight)
			}

			// 如果所有交易都是旧的，需要判断是否继续查找
			// 继续向后查找，直到找到槽位在范围内（scanHeight < slot <= currentSlot）的交易
			// 或者已经查找了很多 tricky（说明没有更多交易了）
			if allOld && processedCount == 0 {
				// 检查这批中的最大槽位（第一个，最新的）
				maxSlotInBatch := uint64(0)
				if len(signatures) > 0 {
					maxSlotInBatch = uint64(signatures[0].Slot) // 第一个是最新的
				}

				// 如果最大槽位还在 scanHeight 附近（说明可能还有更多交易），继续查找
				// 如果最大槽位远小于 scanHeight，可能需要检查是否找到了所有交易
				if maxSlotInBatch > 0 && maxSlotInBatch < scanHeight {
					// 计算差值
					diff := scanHeight - maxSlotInBatch
					if diff > 10000 {
						// 差值太大，说明这些交易已经很旧了，但仍然需要继续查找
						// 因为 GetSignaturesForAddress 可能不按槽位排序
						log.Printf("[DEBUG] 地址 %s 最大槽位 %d 远小于 scanHeight %d (差值: %d)，继续向后查找", addr.Address, maxSlotInBatch, scanHeight, diff)
					} else {
						log.Printf("[DEBUG] 地址 %s 本批都是旧交易(最大槽位: %d)，继续向后查找更多交易", addr.Address, maxSlotInBatch)
					}
				}

				// 如果最小槽位已经很小了，且查找了很多批，考虑停止
				// 但为了安全，我们继续查找，直到获取不到更多签名
			}

			// 使用最后一个签名作为下次的 Before（继续向后查找）
			before = signatures[len(signatures)-1].Signature
			hasBefore = true
			time.Sleep(interval) // 避免 RPC 限流
		}

		// 更新 scan_height 到当前
		latestAddr.ScanHeight = uint64(currentSlot)
		if err := l.db.Save(&latestAddr).Error; err != nil {
			log.Printf("更新地址 %s 的 scan_height 失败: %v", addr.Address, err)
		} else {
			successCount++
			log.Printf("地址 %s 历史同步完成", addr.Address)
		}
	}
	log.Printf("历史同步完成: %d/%d 个地址成功", successCount, len(addresses))
	if successCount == 0 && len(addresses) > 0 {
		return fmt.Errorf("所有地址的历史同步都失败")
	}
	return nil
}

// syncHistoryGap 同步指定起始槽位到当前的所有交易（用于填补历史同步期间的空白）
func (l *Listener) syncHistoryGap(addresses []db.Address, startSlot uint64, interval time.Duration) error {
	// 获取当前最新槽位
	currentSlot, err := l.client.GetSlot(l.ctx, rpc.CommitmentFinalized)
	if err != nil {
		return err
	}

	if uint64(currentSlot) <= startSlot {
		return nil // 没有新交易
	}

	log.Printf("[GAP SYNC] 开始同步槽位 %d 到 %d", startSlot, currentSlot)

	for _, addr := range addresses {
		var latestAddr db.Address
		if err := l.db.Where("address = ?", addr.Address).First(&latestAddr).Error; err != nil {
			continue
		}

		scanHeight := latestAddr.ScanHeight
		if scanHeight >= uint64(currentSlot) {
			continue // 已经是最新
		}

		// 同步从 scanHeight 到 currentSlot 的交易
		limit := 100
		var before solana.Signature
		hasBefore := false
		processedAny := false

		for {
			opts := &rpc.GetSignaturesForAddressOpts{
				Limit: &limit,
			}
			if hasBefore {
				opts.Before = before
			}

			signatures, err := l.client.GetSignaturesForAddressWithOpts(
				l.ctx,
				solana.MustPublicKeyFromBase58(addr.Address),
				opts,
			)
			if err != nil || len(signatures) == 0 {
				break
			}

			foundInRange := false
			for _, sigInfo := range signatures {
				slot := uint64(sigInfo.Slot)
				if slot <= scanHeight {
					continue
				}
				if slot > uint64(currentSlot) {
					continue
				}
				// 槽位在范围内，处理
				foundInRange = true
				processedAny = true
				if err := l.processSignature(sigInfo, addr.Address); err != nil {
					log.Printf("[GAP SYNC] 处理交易 %s 失败: %v", sigInfo.Signature, err)
				}
			}

			if !foundInRange {
				break // 没有找到范围内的交易
			}

			before = signatures[len(signatures)-1].Signature
			hasBefore = true
			time.Sleep(interval)
		}

		if processedAny {
			latestAddr.ScanHeight = uint64(currentSlot)
			l.db.Save(&latestAddr)
			log.Printf("[GAP SYNC] 地址 %s 同步完成，更新到槽位 %d", addr.Address, currentSlot)
		}
	}

	return nil
}

func (l *Listener) subscribeAll(addresses []db.Address) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, addr := range addresses {
		pubkey := solana.MustPublicKeyFromBase58(addr.Address)
		sub, err := l.wsClient.AccountSubscribeWithOpts(
			pubkey,
			rpc.CommitmentFinalized,
			solana.EncodingBase64,
		)
		if err != nil {
			return fmt.Errorf("订阅 %s 失败: %w", addr.Address, err)
		}
		l.subs[addr.Address] = sub

		// 每个订阅一个 goroutine 处理通知
		go l.handleNotifications(sub, addr.Address)
		log.Printf("订阅地址: %s", addr.Address)
	}
	return nil
}

func (l *Listener) handleNotifications(sub *ws.AccountSubscription, address string) {
	maxRetries := 5
	retryCount := 0
	for retryCount < maxRetries {
		_, err := sub.Recv(l.ctx)
		if err != nil {
			log.Printf("通知接收失败 %s: %v，重连中 (第 %d/%d 次)...", address, err, retryCount+1, maxRetries)
			sub.Unsubscribe()
			time.Sleep(time.Duration(retryCount+1) * 5 * time.Second) // 递增重试间隔

			// 重新订阅
			pubkey := solana.MustPublicKeyFromBase58(address)
			newSub, err := l.wsClient.AccountSubscribeWithOpts(
				pubkey,
				rpc.CommitmentFinalized,
				solana.EncodingBase64,
			)
			if err != nil {
				retryCount++
				if retryCount >= maxRetries {
					log.Printf("地址 %s 重连失败，已达最大重试次数", address)
					return
				}
				continue
			}

			// 更新订阅
			l.mu.Lock()
			l.subs[address] = newSub
			l.mu.Unlock()
			sub = newSub
			retryCount = 0 // 重置重试计数
			log.Printf("地址 %s 重连成功", address)
			continue
		}

		// 成功接收，重置重试计数
		retryCount = 0

		// 账户变化：获取最新签名（变化后最新交易）
		limit := 1
		signatures, err := l.client.GetSignaturesForAddressWithOpts(
			l.ctx,
			solana.MustPublicKeyFromBase58(address),
			&rpc.GetSignaturesForAddressOpts{
				Limit: &limit, // 只取最新
			},
		)
		if err != nil || len(signatures) == 0 {
			continue
		}
		sigInfo := signatures[0]

		// 检查槽位 > scan_height（避免重复）
		var latestAddr db.Address
		l.db.Where("address = ?", address).First(&latestAddr)
		if uint64(sigInfo.Slot) <= latestAddr.ScanHeight {
			continue
		}

		if err := l.processSignature(sigInfo, address); err != nil {
			log.Printf("处理通知交易 %s 失败: %v", sigInfo.Signature, err)
		}
	}
}

func (l *Listener) processSignature(sigInfo *rpc.TransactionSignature, address string) error {
	if sigInfo.BlockTime == nil || *sigInfo.BlockTime == 0 {
		log.Printf("[DEBUG] 交易 %s 未确认，跳过", sigInfo.Signature)
		return nil // 未确认
	}

	log.Printf("[DEBUG] 处理交易 %s (地址: %s)", sigInfo.Signature, address)

	// 获取交易详情
	tx, err := l.client.GetTransaction(l.ctx, sigInfo.Signature, &rpc.GetTransactionOpts{
		Encoding: solana.EncodingBase64,
	})
	if err != nil || tx == nil {
		log.Printf("[DEBUG] 获取交易详情失败 %s: %v", sigInfo.Signature, err)
		return err
	}

	// 解析 USDC 转账（允许 PreTokenBalances 为空，即新账户首次接收）
	if tx.Meta == nil {
		log.Printf("[DEBUG] 交易 %s 没有 Meta 信息，跳过", sigInfo.Signature)
		return nil
	}

	if len(tx.Meta.PostTokenBalances) == 0 {
		log.Printf("[DEBUG] 交易 %s 没有 PostTokenBalances，跳过", sigInfo.Signature)
		return nil
	}

	// 查找USDC余额变化并计算金额
	var amount uint64
	addrPubkey := solana.MustPublicKeyFromBase58(address)
	foundUSDCTransfer := false

	log.Printf("[DEBUG] 交易 %s: PreTokenBalances=%d, PostTokenBalances=%d",
		sigInfo.Signature, len(tx.Meta.PreTokenBalances), len(tx.Meta.PostTokenBalances))

	// 通过比较PreTokenBalances和PostTokenBalances来检测USDC转账
	for _, post := range tx.Meta.PostTokenBalances {
		if post.Mint != l.mintPubkey {
			continue
		}

		if post.Owner == nil || *post.Owner != addrPubkey {
			continue
		}

		log.Printf("[DEBUG] 找到目标地址 %s 的 USDC Token 账户，账户索引: %d", address, post.AccountIndex)

		// 查找对应的PreTokenBalance
		var preAmount uint64 = 0
		var postAmount uint64 = 0
		var foundPre bool

		// 从 PostTokenBalances 中获取余额
		if post.UiTokenAmount != nil {
			// UiTokenAmount 包含 UIAmount 和 Decimals
			// UIAmount 已经是格式化后的金额（例如 1.5），需要乘以 10税decimals 得到原始值
			uiAmount := post.UiTokenAmount.UiAmount
			if uiAmount != nil {
				decimals := uint64(post.UiTokenAmount.Decimals)
				if decimals == 0 {
					decimals = 6 // USDC 默认 6 位小数
				}
				postAmount = uint64(*uiAmount * float64(1e6)) // US bus 使用 6 位小数
				log.Printf("[DEBUG] PostTokenBalance: UIAmount=%.6f, Amount=%d", *uiAmount, postAmount)
			}
		}

		// 查找 PreTokenBalance
		for _, pre := range tx.Meta.PreTokenBalances {
			if pre.AccountIndex == post.AccountIndex && pre.Mint == l.mintPubkey {
				foundPre = true
				if pre.UiTokenAmount != nil {
					uiAmount := pre.UiTokenAmount.UiAmount
					if uiAmount != nil {
						preAmount = uint64(*uiAmount * float64(1e6))
					}
					log.Printf("[DEBUG] PreTokenBalance: UIAmount=%.6f, Amount=%d", *uiAmount, preAmount)
				}
				break
			}
		}

		// 计算转账金额
		if postAmount > preAmount {
			amount = postAmount - preAmount
			foundUSDCTransfer = true
			log.Printf("[DEBUG] 检测到 USDC 转账: 金额 = %d (Post: %d, Pre: %d)", amount, postAmount, preAmount)
			break
		} else if !foundPre && postAmount > 0 {
			// 新账户，首次接收
			amount = postAmount
			foundUSDCTransfer = true
			log.Printf("[DEBUG] 新账户首次接收 USDC: 金额 = %d", amount)
			break
		}
	}

	if !foundUSDCTransfer {
		log.Printf("[DEBUG] 交易 %s 未检测到 USDC 转账到目标地址 %s", sigInfo.Signature, address)
		return nil
	}

	if amount == 0 {
		log.Printf("[DEBUG] 交易 %s USDC 转账金额为 0，跳过", sigInfo.Signature)
		return nil
	}

	// 解析 Memo - 从交易日志/指令中提取
	var memo string
	if tx.Transaction != nil && tx.Meta != nil {
		// 优先从日志消息中查找 Memo（Solana 日志常见格式：Program log: Memo (len N): "..."）
		if tx.Meta.LogMessages != nil {
			for _, logMsg := range tx.Meta.LogMessages {
				// 格式1：Program log: Memo (len N): "..."
				if strings.Contains(logMsg, "Program log: Memo") || strings.Contains(logMsg, "Memo (") {
					start := strings.Index(logMsg, "\"")
					end := strings.LastIndex(logMsg, "\"")
					if start >= 0 && end > start {
						memo = logMsg[start+1 : end]
						memo = strings.TrimSpace(memo)
						log.Printf("[DEBUG] 从日志解析到 Memo: %s", memo)
						break
					}
				}
				// 兼容旧格式：... Memo: ...
				if memo == "" && strings.Contains(logMsg, "Memo:") {
					parts := strings.Split(logMsg, "Memo:")
					if len(parts) > 1 {
						memo = strings.TrimSpace(parts[1])
						log.Printf("[DEBUG] 从日志解析到 Memo: %s", memo)
						break
					}
				}
			}
		}

		// 如果日志中没有 memo，可在此处补充从交易指令解析（当前先跳过，日志记录）
		if memo == "" {
			log.Printf("[DEBUG] 日志未解析到 Memo，暂不从指令解析")
		}
	}

	if memo == "" {
		log.Printf("[WARN] 交易 %s 没有 Memo，跳过保存（地址: %s, 金额: %d）", sigInfo.Signature, address, amount)
		return nil // 没有memo，跳过
	}

	// 直接将完整的 Memo 作为 OrderID 保存
	orderID := strings.TrimSpace(memo)

	// 保存交易（将完整的 Memo 保存为 OrderID）
	txRecord := &db.Transaction{
		OrderID:     orderID,
		Address:     address,
		Amount:      amount,
		TXSignature: sigInfo.Signature.String(),
		BlockHeight: uint64(sigInfo.Slot),
		Status:      "confirmed",
	}
	if err := db.SaveTransaction(l.db, txRecord); err != nil {
		return err
	}
	log.Printf("处理交易 %s: 订单 %s, 金额 %d", txRecord.TXSignature, orderID, amount)

	// 更新 scan_height
	var latestAddr db.Address
	l.db.Where("address = ?", address).First(&latestAddr)
	if uint64(sigInfo.Slot) > latestAddr.ScanHeight {
		latestAddr.ScanHeight = uint64(sigInfo.Slot)
		l.db.Save(&latestAddr)
	}

	return nil
}

func (l *Listener) unsubscribeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, sub := range l.subs {
		sub.Unsubscribe()
	}
	l.subs = make(map[string]*ws.AccountSubscription)
}

// parseMemo 保持不变
func parseMemo(data []byte) string {
	return string(data) // 实际需 base58 解码或处理
}

// getUSDCAccountsForOwner 返回 owner 的 USDC token 账户列表
func (l *Listener) getUSDCAccountsForOwner(owner string) ([]solana.PublicKey, error) {
	ownerPk := solana.MustPublicKeyFromBase58(owner)
	out, err := l.client.GetTokenAccountsByOwner(l.ctx, ownerPk, &rpc.GetTokenAccountsConfig{
		Mint: &l.mintPubkey,
	}, &rpc.GetTokenAccountsOpts{
		Encoding: solana.EncodingBase64,
	})
	if err != nil {
		return nil, err
	}
	var res []solana.PublicKey
	for _, it := range out.Value {
		res = append(res, it.Pubkey)
	}
	return res, nil
}
