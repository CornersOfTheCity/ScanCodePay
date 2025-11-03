package services

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/spf13/viper"

	"ScanCodePay/utils"
)

var (
	Client   *rpc.Client
	Payer    solana.PrivateKey
	Refund   solana.PrivateKey // 退款账户私钥
	USDCmint solana.PublicKey  // USDC mint 地址
	// txMutex 用于保护并发交易发送，虽然 Solana 没有 nonce 问题，
	// 但为了避免 RPC 节点限制和账户余额问题，使用互斥锁确保顺序处理
	txMutex sync.Mutex
)

var (
	ErrInvalidRequest       = errors.New("invalid request")
	ErrBadTx                = errors.New("bad tx")
	ErrPartialSignFailed    = errors.New("partial sign failed")
	ErrEncodeFailed         = errors.New("encode failed")
	ErrSerializeFailed      = errors.New("serialize failed")
	ErrBroadcastFailed      = errors.New("broadcast failed")
	ErrOrderNotFound        = errors.New("order not found")
	ErrRefundAmountExceeded = errors.New("refund amount exceeded payment amount")
)

// InitSolana initializes RPC client and payer from config file.
// It reads solana.rpc_url and solana.payer_secret from the config.
// The payer_secret must be in base58 format.
func InitSolana() error {
	rpcURL := viper.GetString("solana.rpc_url")
	if rpcURL == "" {
		return errors.New("solana.rpc_url is empty in config")
	}

	payerSecret := viper.GetString("solana.payer_secret")
	if payerSecret == "" {
		return errors.New("solana.payer_secret is empty in config")
	}

	refundSecret := viper.GetString("solana.refund_secret")
	if refundSecret == "" {
		return errors.New("solana.refund_secret is empty in config")
	}

	usdcMint := viper.GetString("solana.usdc_mint")
	if usdcMint == "" {
		return errors.New("solana.usdc_mint is empty in config")
	}

	Client = rpc.New(rpcURL)

	// Only support base58 format
	pk, err := solana.PrivateKeyFromBase58(payerSecret)
	if err != nil {
		return errors.New("failed to parse payer_secret as base58: " + err.Error())
	}
	Payer = pk

	// 解析退款账户私钥
	refundPk, err := solana.PrivateKeyFromBase58(refundSecret)
	if err != nil {
		return errors.New("failed to parse refund_secret as base58: " + err.Error())
	}
	Refund = refundPk

	// 解析 USDC mint 地址
	usdcPubkey, err := solana.PublicKeyFromBase58(usdcMint)
	if err != nil {
		return errors.New("failed to parse usdc_mint as base58: " + err.Error())
	}
	USDCmint = usdcPubkey

	return nil
}

// GetPayerAddress 返回用于签名的账户地址（Payer 的公钥地址）
func GetPayerAddress() string {
	var pubkey solana.PublicKey
	var ok bool

	// 使用匿名函数和 recover 安全地获取公钥
	func() {
		defer func() {
			if r := recover(); r != nil {
				// panic 被捕获，说明 Payer 未正确初始化
				ok = false
			}
		}()
		pubkey = Payer.PublicKey()
		ok = true
	}()

	if !ok || pubkey.IsZero() {
		return ""
	}
	return pubkey.String()
}

// GetRefundAddress 返回退款账户地址（Refund 的公钥地址）
func GetRefundAddress() string {
	var pubkey solana.PublicKey
	var ok bool

	// 使用匿名函数和 recover 安全地获取公钥
	func() {
		defer func() {
			if r := recover(); r != nil {
				// panic 被捕获，说明 Refund 未正确初始化
				ok = false
			}
		}()
		pubkey = Refund.PublicKey()
		ok = true
	}()

	if !ok || pubkey.IsZero() {
		return ""
	}
	return pubkey.String()
}

// SignTx: 接收用户已签名的交易，用服务端私钥进行部分签名后直接广播到链上
// 返回交易签名和 explorer URL
func SignTx(ctx context.Context, serializedTx string) (string, string, error) {
	if serializedTx == "" {
		return "", "", ErrInvalidRequest
	}

	// 解码用户已签名的交易
	tx, err := utils.DecodeBase64Tx(serializedTx)
	if err != nil {
		return "", "", ErrBadTx
	}

	// 记录 RPC 节点信息（用于调试）
	rpcURL := viper.GetString("solana.rpc_url")
	fmt.Printf("[DEBUG] 当前使用的 RPC 节点: %s\n", rpcURL)

	// 调试日志：检查交易状态
	fmt.Printf("[DEBUG] 反序列化后的交易状态:\n")
	fmt.Printf("  - AccountKeys 数量: %d\n", len(tx.Message.AccountKeys))
	fmt.Printf("  - Signatures 数量: %d\n", len(tx.Signatures))
	fmt.Printf("  - RequiredSignatures: %d\n", tx.Message.Header.NumRequiredSignatures)
	if len(tx.Message.AccountKeys) > 0 {
		fmt.Printf("  - FeePayer (AccountKeys[0]): %s\n", tx.Message.AccountKeys[0].String())
	}
	for i, sig := range tx.Signatures {
		fmt.Printf("  - Signature[%d]: %s (IsZero: %v)\n", i, sig.String(), sig.IsZero())
	}
	if len(tx.Message.AccountKeys) > 1 {
		fmt.Printf("  - User Pubkey (AccountKeys[1]): %s\n", tx.Message.AccountKeys[1].String())
	}

	// 验证交易是否需要服务端签名（fee payer 是否是服务端账户）
	feePayerPubkey := Payer.PublicKey()
	if len(tx.Message.AccountKeys) == 0 {
		return "", "", ErrBadTx
	}

	// Fee payer 是第一个账户（索引 0）
	feePayer := tx.Message.AccountKeys[0]

	// 验证 fee payer 必须是服务端账户（因为用户已指定代付 gas 的账户）
	if !feePayer.Equals(feePayerPubkey) {
		return "", "", ErrBadTx
	}

	// 检查签名状态
	requiredSigners := int(tx.Message.Header.NumRequiredSignatures)

	// 确保 Signatures 数组有足够的空间
	if len(tx.Signatures) < requiredSigners {
		// 扩展 Signatures 数组
		for len(tx.Signatures) < requiredSigners {
			tx.Signatures = append(tx.Signatures, solana.Signature{})
		}
	}

	// 检查 feePayer 是否需要签名（Signatures[0] 对应 feePayer）
	// feePayer 必须是第一个账户（AccountKeys[0]）
	needsPayerSigning := len(tx.Signatures) == 0 || tx.Signatures[0].IsZero()

	// 检查 blockhash 是否有效（在签名前检查）
	// 注意：Solana 节点通常接受 150 个区块内的 blockhash（约 60-90 秒）
	// 我们不应该在这里就拒绝稍微过期的 blockhash，应该让 RPC 节点来判断
	currentBh, err := Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
	if err != nil {
		return "", "", ErrBadTx
	}

	fmt.Printf("[DEBUG] 当前 RPC 节点返回的最新 blockhash: %s (LastValidBlockHeight: %d)\n",
		currentBh.Value.Blockhash.String(), currentBh.Context.Slot)
	fmt.Printf("[DEBUG] 交易中的 blockhash: %s\n", tx.Message.RecentBlockhash.String())

	// 如果 blockhash 为零，更新为最新（这种情况下交易本身可能有问题，但尝试修复）
	if tx.Message.RecentBlockhash.IsZero() {
		tx.Message.RecentBlockhash = currentBh.Value.Blockhash
		fmt.Printf("[DEBUG] Blockhash 为零，更新为最新: %s\n", currentBh.Value.Blockhash.String())
	} else if tx.Message.RecentBlockhash != currentBh.Value.Blockhash {
		// Blockhash 不同于当前最新的，但不一定是过期的
		// Solana 节点通常接受 150 个区块内的 blockhash，让 RPC 节点来判断
		// 我们保留原 blockhash，继续处理交易
		fmt.Printf("[DEBUG] Blockhash 不同于当前最新（交易中: %s, 当前: %s），但继续尝试广播（Solana 节点会判断是否接受）\n",
			tx.Message.RecentBlockhash.String(), currentBh.Value.Blockhash.String())
		// 注意：我们不更新 blockhash，因为用户签名基于旧的 blockhash
		// 如果 blockhash 真的过期，RPC 节点会在广播时返回错误
	}

	// 如果需要 feePayer 签名，添加服务端签名
	if needsPayerSigning {
		// 尝试使用 tx.Sign() 方法进行部分签名
		// 关键：需要先验证用户的签名是否有效（基于当前消息）
		// 如果用户签名无效，整个交易会失败

		// 保存用户的签名（如果存在）
		savedUserSig := solana.Signature{}
		if len(tx.Signatures) > 1 && !tx.Signatures[1].IsZero() {
			savedUserSig = tx.Signatures[1]
			fmt.Printf("[DEBUG] 保存用户签名: %s\n", savedUserSig.String())
		}

		// 使用 tx.Sign() 方法，但只为 feePayer 提供私钥
		// 注意：如果用户的签名已经有效，tx.Sign() 应该能够识别并保留它
		signResult, err := tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
			if pk.Equals(feePayerPubkey) {
				fmt.Printf("[DEBUG] 为 feePayer %s 提供私钥进行签名\n", pk.String())
				return &Payer
			}
			// 对于其他签名者，返回 nil
			// 如果用户已经签名，签名应该已经存在于 Signatures 数组中
			return nil
		})

		if err != nil {
			fmt.Printf("[DEBUG] tx.Sign() 失败: %v\n", err)

			// 如果 tx.Sign() 失败，尝试手动签名作为回退方案
			// 注意：Solana 交易签名是对消息的序列化字节直接进行 Ed25519 签名
			// Ed25519.Sign() 方法内部会对消息进行 SHA-512 哈希，我们不需要手动做 SHA256
			fmt.Printf("[DEBUG] 尝试手动签名作为回退方案...\n")
			messageBytes, err2 := tx.Message.MarshalBinary()
			if err2 != nil {
				return "", "", fmt.Errorf("%w: 序列化消息失败: %v", ErrPartialSignFailed, err2)
			}

			// Solana Ed25519 签名：直接对消息字节进行签名（Ed25519 内部会处理哈希）
			// 注意：Payer.Sign() 使用的是 Ed25519 签名，不是 SHA256 + 签名
			feePayerSig, err2 := Payer.Sign(messageBytes)
			if err2 != nil {
				return "", "", fmt.Errorf("%w: feePayer 手动签名失败: %v (tx.Sign 错误: %v)", ErrPartialSignFailed, err2, err)
			}

			tx.Signatures[0] = feePayerSig
			// 恢复用户签名
			if !savedUserSig.IsZero() {
				tx.Signatures[1] = savedUserSig
			}
		} else {
			fmt.Printf("[DEBUG] tx.Sign() 成功，返回 %d 个签名\n", len(signResult))
			// 检查用户签名是否被保留
			if !savedUserSig.IsZero() && len(tx.Signatures) > 1 {
				if tx.Signatures[1].IsZero() {
					fmt.Printf("[DEBUG] 警告: 用户签名被清除，尝试恢复...\n")
					tx.Signatures[1] = savedUserSig
				} else if tx.Signatures[1].String() != savedUserSig.String() {
					fmt.Printf("[DEBUG] 警告: 用户签名发生变化，使用保存的签名\n")
					tx.Signatures[1] = savedUserSig
				}
			}
		}

		// 调试日志：检查签名后的状态
		fmt.Printf("[DEBUG] feePayer 签名后状态:\n")
		fmt.Printf("  - Blockhash: %s\n", tx.Message.RecentBlockhash.String())
		fmt.Printf("  - FeePayer Signature[0]: %s (IsZero: %v)\n", tx.Signatures[0].String(), tx.Signatures[0].IsZero())
		if len(tx.Signatures) > 1 {
			fmt.Printf("  - User Signature[1]: %s (IsZero: %v)\n", tx.Signatures[1].String(), tx.Signatures[1].IsZero())
		}
	}

	// 使用互斥锁保护交易广播，避免高并发时的问题
	// 注意：Solana 虽然没有 nonce 问题，但使用锁可以：
	// 1. 避免 RPC 节点对并发请求的限制
	// 2. 确保账户余额充足（避免同时发送大量交易导致余额不足）
	// 3. 减少 blockhash 过期等问题
	txMutex.Lock()
	defer txMutex.Unlock()

	// 在持有锁后，再次检查 blockhash（防止在等待锁的过程中 blockhash 过期）
	// 注意：我们不更新 blockhash，因为这会使用户签名失效
	// Solana 节点通常会接受稍微过期的 blockhash（通常在150个区块内）
	bh, err := Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
	if err != nil {
		return "", "", ErrBadTx
	}

	// 仅记录 blockhash 状态，不更新
	if tx.Message.RecentBlockhash != bh.Value.Blockhash {
		fmt.Printf("[DEBUG] 警告: Blockhash 已变化（交易中: %s, 当前: %s），但保留原 blockhash 以保持签名有效\n",
			tx.Message.RecentBlockhash.String(), bh.Value.Blockhash.String())
	}

	// 序列化交易前，再次检查签名状态
	fmt.Printf("[DEBUG] 序列化前最终状态:\n")
	fmt.Printf("  - Signatures 数量: %d\n", len(tx.Signatures))
	for i, sig := range tx.Signatures {
		fmt.Printf("  - Signature[%d]: %s (IsZero: %v)\n", i, sig.String(), sig.IsZero())
	}

	// 序列化交易
	enc, err := tx.MarshalBinary()
	if err != nil {
		return "", "", ErrSerializeFailed
	}
	fmt.Printf("[DEBUG] 序列化成功，交易大小: %d 字节\n", len(enc))

	// 广播交易（带重试机制）
	// 参考 SolanaRelayService 的实现：使用 skipPreflight: true 跳过预检
	// 这样可以避免 blockhash 验证问题（即使 blockhash 稍微过期，只要在链上有效就可以接受）
	// 注意：gagliardetto/solana-go 的 SendRawTransaction 不支持 skipPreflight 选项
	// 我们需要使用底层 RPC 调用来实现这个功能
	var sig solana.Signature
	var broadcastErr error
	maxRetries := 3
	blockhashNotFound := false

	// 参考 SolanaRelayService：如果 blockhash 已过期，重试不会有效，应该直接返回错误
	for i := 0; i < maxRetries; i++ {
		fmt.Printf("[DEBUG] 尝试广播交易 (尝试 %d/%d，使用 skipPreflight: true)...\n", i+1, maxRetries)

		// 参考 SolanaRelayService：使用 skipPreflight: true 跳过预检
		// 这允许即使 blockhash 稍微过期（在150个区块内）的交易也能被接受
		// 使用底层 RPC 调用发送交易，支持 skipPreflight 选项
		// 注意：Solana RPC 的 sendTransaction 接受 base64 编码的字符串
		encBase64 := base64.StdEncoding.EncodeToString(enc)
		broadcastErr = Client.RPCCallForInto(ctx, &sig, "sendTransaction", []interface{}{
			encBase64, // base64 编码的字符串
			map[string]interface{}{
				"skipPreflight":       true,        // 跳过预检（关键！）
				"preflightCommitment": "confirmed", // 预检承诺级别
				"encoding":            "base64",    // 编码格式
			},
		})

		if broadcastErr == nil {
			fmt.Printf("[DEBUG] 广播成功！交易签名: %s\n", sig.String())
			break
		}

		fmt.Printf("[DEBUG] 广播失败 (尝试 %d/%d): %v\n", i+1, maxRetries, broadcastErr)

		// 检查是否是 "Blockhash not found" 错误
		// 参考 SolanaRelayService 的实现逻辑：如果是 blockhash 过期，直接返回，不重试
		errStr := broadcastErr.Error()
		if strings.Contains(errStr, "Blockhash not found") || strings.Contains(errStr, "BlockhashNotFound") {
			blockhashNotFound = true
			fmt.Printf("[DEBUG] 检测到 Blockhash 已过期（类似 SolanaRelayService 的行为：停止重试并返回错误）\n")
			break
		}

		// 对于其他错误（如网络错误、账户余额不足等），继续重试
		// 注意：Solana 交易失败通常很快，不需要长时间等待
	}

	if broadcastErr != nil {
		// 提供更详细的错误信息
		errorDetail := fmt.Sprintf("广播失败: %v", broadcastErr)

		// 根据不同的错误类型提供更友好的提示
		errStr := broadcastErr.Error()
		if blockhashNotFound || strings.Contains(errStr, "Blockhash not found") || strings.Contains(errStr, "BlockhashNotFound") {
			// Blockhash 已过期，这是最常见的问题
			// 可能原因：
			// 1. 构造交易时使用的 blockhash 在用户签名和广播之间已过期
			// 2. SolanaRelayService 使用的 blockhash 已经过期（超过150个区块，约60-90秒）
			// 3. 用户签名过程耗时过长，导致 blockhash 过期
			errorDetail = fmt.Sprintf("blockhash 已过期或不存在（交易中的 blockhash: %s，当前 RPC 节点: %s）。解决方案：请在前端重新调用 SolanaRelayService 的构造交易接口获取最新的 blockhash，然后重新构造交易并签名，再调用 signTx 接口", tx.Message.RecentBlockhash.String(), rpcURL)
		} else if strings.Contains(errStr, "signature verification failure") {
			errorDetail = "签名验证失败，可能原因：1) blockhash 已过期（请重新获取最新 blockhash 并重新签名）；2) 用户签名无效；3) 交易格式不正确"
		} else {
			errorDetail += fmt.Sprintf(" | 重试次数: %d/%d", maxRetries, maxRetries)
		}

		fmt.Printf("[DEBUG] 最终广播失败: %s\n", errorDetail)
		return "", "", fmt.Errorf("%w: %s", ErrBroadcastFailed, errorDetail)
	}

	signature := sig.String()
	explorerURL := "https://explorer.solana.com/tx/" + signature + "?cluster=mainnet"

	return signature, explorerURL, nil
}

// // BroadcastTx: send raw tx to chain and return signature
// func BroadcastTx(ctx context.Context, serializedTx string) (string, error) {
// 	if serializedTx == "" {
// 		return "", ErrInvalidRequest
// 	}

// 	tx, err := utils.DecodeBase64Tx(serializedTx)
// 	if err != nil {
// 		return "", ErrBadTx
// 	}

// 	enc, err := tx.MarshalBinary()
// 	if err != nil {
// 		return "", ErrSerializeFailed
// 	}

// 	sig, err := Client.SendRawTransaction(ctx, enc)
// 	if err != nil {
// 		return "", ErrBroadcastFailed
// 	}

// 	return sig.String(), nil
// }

// CreateRefundTx 创建退款交易并广播
// orderID: 原收款订单ID
// refundTo: 退款收款人地址
// refundAmount: 退款金额（lamports）
// 返回交易签名和 explorer URL
func CreateRefundTx(ctx context.Context, orderID, refundTo string, refundAmount uint64) (string, string, error) {
	fmt.Printf("[DEBUG] CreateRefundTx 调用: orderID=%s, refundTo=%s, refundAmount=%d\n", orderID, refundTo, refundAmount)

	// 验证参数
	if orderID == "" || refundTo == "" || refundAmount == 0 {
		fmt.Printf("[DEBUG] 参数验证失败: orderID 为空或 refundTo 为空或 refundAmount 为 0\n")
		return "", "", ErrInvalidRequest
	}

	// 验证退款收款人地址格式
	refundToPubkey, err := solana.PublicKeyFromBase58(refundTo)
	if err != nil {
		fmt.Printf("[DEBUG] 退款收款人地址格式错误: %v\n", err)
		return "", "", ErrInvalidRequest
	}
	fmt.Printf("[DEBUG] 退款收款人地址验证成功: %s\n", refundToPubkey.String())

	// 获取退款账户（Refund）的 USDC Token Account
	refundPubkey := Refund.PublicKey()
	fmt.Printf("[DEBUG] 退款账户公钥: %s\n", refundPubkey.String())
	tokenAccounts, err := Client.GetTokenAccountsByOwner(ctx, refundPubkey, &rpc.GetTokenAccountsConfig{
		Mint: &USDCmint,
	}, &rpc.GetTokenAccountsOpts{
		Encoding: solana.EncodingBase64,
	})
	if err != nil {
		fmt.Printf("[DEBUG] 获取退款账户 Token Account 失败: %v\n", err)
		return "", "", fmt.Errorf("failed to get refund account's USDC token account: %v", err)
	}
	if len(tokenAccounts.Value) == 0 {
		fmt.Printf("[DEBUG] 退款账户没有 USDC Token Account\n")
		return "", "", errors.New("refund account does not have USDC token account")
	}
	sourceTokenAccount := tokenAccounts.Value[0].Pubkey
	fmt.Printf("[DEBUG] 退款账户 USDC Token Account: %s\n", sourceTokenAccount.String())

	// 获取或创建收款人的 USDC Token Account
	var destTokenAccount solana.PublicKey
	destAccounts, err := Client.GetTokenAccountsByOwner(ctx, refundToPubkey, &rpc.GetTokenAccountsConfig{
		Mint: &USDCmint,
	}, &rpc.GetTokenAccountsOpts{
		Encoding: solana.EncodingBase64,
	})
	if err != nil {
		fmt.Printf("[DEBUG] 获取收款人 Token Account 失败: %v\n", err)
		return "", "", fmt.Errorf("failed to get recipient's USDC token account: %v", err)
	}
	if len(destAccounts.Value) == 0 {
		// 如果没有 Token Account，需要创建（这里简化处理，实际可能需要关联账户）
		fmt.Printf("[DEBUG] 收款人没有 USDC Token Account: %s\n", refundToPubkey.String())
		return "", "", errors.New("recipient does not have USDC token account")
	}
	destTokenAccount = destAccounts.Value[0].Pubkey
	fmt.Printf("[DEBUG] 收款人 USDC Token Account: %s\n", destTokenAccount.String())

	// 获取最新 blockhash
	// 注意：使用 CommitmentFinalized 而不是 CommitmentConfirmed，以获得更稳定的 blockhash
	// 在持有锁后，我们会再次检查并更新 blockhash（如果需要）
	bh, err := Client.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		// 如果 Finalized 失败，尝试 Confirmed
		fmt.Printf("[DEBUG] 获取 Finalized blockhash 失败，尝试 Confirmed: %v\n", err)
		bh, err = Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
		if err != nil {
			return "", "", errors.New("failed to get latest blockhash")
		}
	}
	fmt.Printf("[DEBUG] 获取 blockhash: %s (commitment: %s)\n", bh.Value.Blockhash.String(), "Finalized/Confirmed")

	// 构建 USDC 转账指令（使用 Token Program）
	// Token Program ID: TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
	tokenProgramID := solana.MustPublicKeyFromBase58("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

	// Token Transfer 指令格式：
	// instruction discriminator: 3 (Transfer)
	// amount: 8 bytes (uint64, little-endian)
	amountBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(amountBytes, refundAmount)
	transferData := append([]byte{3}, amountBytes...)

	// 构建账户列表
	accounts := solana.AccountMetaSlice{
		{PublicKey: sourceTokenAccount, IsSigner: false, IsWritable: true}, // Source
		{PublicKey: destTokenAccount, IsSigner: false, IsWritable: true},   // Destination
		{PublicKey: refundPubkey, IsSigner: true, IsWritable: false},       // Owner (authority)
	}

	// 创建转账指令
	transferInstruction := solana.NewInstruction(
		tokenProgramID,
		accounts,
		transferData,
	)

	// 创建 Memo 指令（格式：refund:orderId）
	// Memo Program ID: MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr
	memoProgramID := solana.MustPublicKeyFromBase58("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
	memoText := fmt.Sprintf("refund:%s", orderID)
	memoInstruction := solana.NewInstruction(
		memoProgramID,
		solana.AccountMetaSlice{},
		[]byte(memoText),
	)

	// 创建交易，包含转账和Memo指令，使用Payer作为 fee payer（统一所有交易的fee payer）
	// 这样所有USDC交易（收款和退款）都可以通过监听Payer地址统一捕获
	payerPubkey := Payer.PublicKey()
	tx, err := solana.NewTransaction(
		[]solana.Instruction{transferInstruction, memoInstruction},
		bh.Value.Blockhash,
		solana.TransactionPayer(payerPubkey),
	)
	if err != nil {
		return "", "", errors.New("failed to create transaction")
	}

	// 签名交易
	// 注意：需要Refund账户签名（USDC转账的authority）和Payer账户签名（fee payer）
	fmt.Printf("[DEBUG] 开始签名退款交易...\n")
	fmt.Printf("[DEBUG] 需要签名的账户: Refund=%s, Payer=%s\n", refundPubkey.String(), payerPubkey.String())
	_, err = tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
		if pk.Equals(refundPubkey) {
			// Refund账户签名：USDC转账的authority
			fmt.Printf("[DEBUG] 为 Refund 账户 %s 提供私钥\n", pk.String())
			return &Refund
		}
		if pk.Equals(payerPubkey) {
			// Payer账户签名：fee payer
			fmt.Printf("[DEBUG] 为 Payer 账户 %s 提供私钥\n", pk.String())
			return &Payer
		}
		return nil
	})
	if err != nil {
		fmt.Printf("[DEBUG] 签名失败: %v\n", err)
		return "", "", fmt.Errorf("%w: %v", ErrPartialSignFailed, err)
	}
	fmt.Printf("[DEBUG] 签名成功，签名数量: %d\n", len(tx.Signatures))

	// 序列化交易
	fmt.Printf("[DEBUG] 序列化退款交易...\n")
	enc, err := tx.MarshalBinary()
	if err != nil {
		fmt.Printf("[DEBUG] 序列化失败: %v\n", err)
		return "", "", ErrSerializeFailed
	}
	fmt.Printf("[DEBUG] 序列化成功，交易大小: %d 字节\n", len(enc))

	// 使用互斥锁保护交易广播
	txMutex.Lock()
	defer txMutex.Unlock()

	// 在持有锁后，再次检查 blockhash（防止在等待锁的过程中 blockhash 过期）
	// 使用 Finalized commitment 以获得更稳定的 blockhash
	currentBh, err := Client.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		// 如果 Finalized 失败，尝试 Confirmed
		fmt.Printf("[DEBUG] 获取 Finalized blockhash 失败，尝试 Confirmed: %v\n", err)
		currentBh, err = Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
		if err != nil {
			return "", "", errors.New("failed to get latest blockhash before broadcast")
		}
	}

	// 如果 blockhash 已变化，需要更新并重新签名
	if tx.Message.RecentBlockhash != currentBh.Value.Blockhash {
		fmt.Printf("[DEBUG] Blockhash 已变化（交易中: %s, 当前: %s），更新并重新签名\n",
			tx.Message.RecentBlockhash.String(), currentBh.Value.Blockhash.String())

		// 更新 blockhash
		tx.Message.RecentBlockhash = currentBh.Value.Blockhash

		// 重新签名交易
		_, err = tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
			if pk.Equals(refundPubkey) {
				fmt.Printf("[DEBUG] 重新签名: 为 Refund 账户 %s 提供私钥\n", pk.String())
				return &Refund
			}
			if pk.Equals(payerPubkey) {
				fmt.Printf("[DEBUG] 重新签名: 为 Payer 账户 %s 提供私钥\n", pk.String())
				return &Payer
			}
			return nil
		})
		if err != nil {
			fmt.Printf("[DEBUG] 重新签名失败: %v\n", err)
			return "", "", fmt.Errorf("%w: blockhash 更新后重新签名失败: %v", ErrPartialSignFailed, err)
		}
		fmt.Printf("[DEBUG] 重新签名成功，签名数量: %d\n", len(tx.Signatures))
		for i, sig := range tx.Signatures {
			fmt.Printf("[DEBUG] 重新签名后 Signature[%d]: %s (IsZero: %v)\n", i, sig.String(), sig.IsZero())
		}

		// 重新序列化交易
		enc, err = tx.MarshalBinary()
		if err != nil {
			fmt.Printf("[DEBUG] 重新序列化失败: %v\n", err)
			return "", "", ErrSerializeFailed
		}
		fmt.Printf("[DEBUG] 重新序列化成功，交易大小: %d 字节\n", len(enc))
	}

	// 广播交易（带重试机制）
	// 每次重试都重新获取 blockhash 并重新签名，避免 blockhash 过期问题
	var sig solana.Signature
	var broadcastErr error
	maxRetries := 3
	fmt.Printf("[DEBUG] 开始广播退款交易 (订单: %s, 金额: %d, 收款人: %s)\n", orderID, refundAmount, refundTo)
	for i := 0; i < maxRetries; i++ {
		// 在每次重试前（从第二次开始），都重新获取最新 blockhash
		// 使用 Finalized commitment 以获得更稳定的 blockhash
		if i > 0 {
			fmt.Printf("[DEBUG] 重试前重新获取 blockhash (尝试 %d/%d)...\n", i+1, maxRetries)
			retryBh, err2 := Client.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
			if err2 != nil {
				// 如果 Finalized 失败，尝试 Confirmed
				fmt.Printf("[DEBUG] 获取 Finalized blockhash 失败，尝试 Confirmed: %v\n", err2)
				retryBh, err2 = Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
			}
			if err2 == nil {
				if tx.Message.RecentBlockhash != retryBh.Value.Blockhash {
					fmt.Printf("[DEBUG] 更新 blockhash 从 %s 到 %s\n",
						tx.Message.RecentBlockhash.String(), retryBh.Value.Blockhash.String())
					tx.Message.RecentBlockhash = retryBh.Value.Blockhash

					// 重新签名
					_, err2 = tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
						if pk.Equals(refundPubkey) {
							return &Refund
						}
						if pk.Equals(payerPubkey) {
							return &Payer
						}
						return nil
					})
					if err2 != nil {
						fmt.Printf("[DEBUG] 重试前重新签名失败: %v\n", err2)
						// 签名失败时，仍然尝试广播（可能签名还是有效的）
					} else {
						fmt.Printf("[DEBUG] 重试前重新签名成功，签名数量: %d\n", len(tx.Signatures))
					}

					// 重新序列化
					enc, err2 = tx.MarshalBinary()
					if err2 != nil {
						fmt.Printf("[DEBUG] 重试前重新序列化失败: %v\n", err2)
						// 序列化失败时，跳过此次重试
						broadcastErr = fmt.Errorf("序列化失败: %v", err2)
						continue
					}
					fmt.Printf("[DEBUG] 重试前更新完成，交易大小: %d 字节\n", len(enc))
				} else {
					fmt.Printf("[DEBUG] Blockhash 未变化，使用当前 blockhash: %s\n", retryBh.Value.Blockhash.String())
				}
			} else {
				fmt.Printf("[DEBUG] 重试前获取 blockhash 失败: %v\n", err2)
			}
		}

		fmt.Printf("[DEBUG] 尝试广播退款交易 (尝试 %d/%d, blockhash: %s)...\n", i+1, maxRetries, tx.Message.RecentBlockhash.String())
		sig, broadcastErr = Client.SendRawTransaction(ctx, enc)
		if broadcastErr == nil {
			fmt.Printf("[DEBUG] 退款交易广播成功！签名: %s\n", sig.String())
			break
		}
		fmt.Printf("[DEBUG] 退款交易广播失败 (尝试 %d/%d): %v\n", i+1, maxRetries, broadcastErr)
	}

	if broadcastErr != nil {
		// 提供更详细的错误信息
		errorDetail := fmt.Sprintf("广播失败: %v", broadcastErr)
		errorDetail += fmt.Sprintf(" | 重试次数: %d/%d", maxRetries, maxRetries)

		// 检查是否是签名验证失败
		errStr := broadcastErr.Error()
		if strings.Contains(errStr, "signature verification failure") {
			errorDetail += " | 提示: 签名验证失败"
		}

		fmt.Printf("[DEBUG] 最终广播失败: %s\n", errorDetail)
		return "", "", fmt.Errorf("%w: %s", ErrBroadcastFailed, errorDetail)
	}

	signature := sig.String()
	explorerURL := "https://explorer.solana.com/tx/" + signature + "?cluster=mainnet"

	return signature, explorerURL, nil
}
