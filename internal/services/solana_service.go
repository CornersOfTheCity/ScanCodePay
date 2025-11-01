package services

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/spf13/viper"

	"ScanCodePay/utils"
)

var (
	Client      *rpc.Client
	Payer       solana.PrivateKey
	RefundPayer solana.PrivateKey // 退款账户私钥
	USDCmint    solana.PublicKey  // USDC mint 地址
	// txMutex 用于保护并发交易发送，虽然 Solana 没有 nonce 问题，
	// 但为了避免 RPC 节点限制和账户余额问题，使用互斥锁确保顺序处理
	txMutex sync.Mutex
)

var (
	ErrInvalidRequest    = errors.New("invalid request")
	ErrBadTx             = errors.New("bad tx")
	ErrPartialSignFailed = errors.New("partial sign failed")
	ErrEncodeFailed      = errors.New("encode failed")
	ErrSerializeFailed   = errors.New("serialize failed")
	ErrBroadcastFailed   = errors.New("broadcast failed")
	ErrOrderNotFound     = errors.New("order not found")
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
	RefundPayer = refundPk

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

	// 检查是否需要服务端签名
	// Fee payer 的签名应该在 Signatures[0]
	// 检查是否已经签名（签名非零且有效）
	needsSigning := len(tx.Signatures) == 0 || tx.Signatures[0].IsZero()

	// 如果需要签名，添加服务端签名
	if needsSigning {
		// 更新 blockhash 如果需要
		if tx.Message.RecentBlockhash.IsZero() {
			bh, err := Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
			if err != nil {
				return "", "", ErrBadTx
			}
			tx.Message.RecentBlockhash = bh.Value.Blockhash
		}

		// 部分签名（只对服务端账户签名，保留用户已有的签名）
		if _, err := tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
			if pk.Equals(feePayerPubkey) {
				return &Payer
			}
			return nil
		}); err != nil {
			return "", "", ErrPartialSignFailed
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
	bh, err := Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
	if err != nil {
		return "", "", ErrBadTx
	}
	
	// 如果 blockhash 已变化，需要更新并重新签名
	if tx.Message.RecentBlockhash != bh.Value.Blockhash {
		tx.Message.RecentBlockhash = bh.Value.Blockhash
		if needsSigning {
			// 重新签名
			if _, err := tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
				if pk.Equals(feePayerPubkey) {
					return &Payer
				}
				return nil
			}); err != nil {
				return "", "", ErrPartialSignFailed
			}
		}
	}

	// 序列化交易
	enc, err := tx.MarshalBinary()
	if err != nil {
		return "", "", ErrSerializeFailed
	}

	// 广播交易（带重试机制）
	var sig solana.Signature
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		sig, err = Client.SendRawTransaction(ctx, enc)
		if err == nil {
			break
		}
		// 如果不是最后一个重试，可以添加短暂延迟后重试
		// 注意：Solana 交易失败通常很快，不需要长时间等待
	}

	if err != nil {
		return "", "", ErrBroadcastFailed
	}

	signature := sig.String()
	explorerURL := "https://explorer.solana.com/tx/" + signature + "?cluster=mainnet"

	return signature, explorerURL, nil
}

// BroadcastTx: send raw tx to chain and return signature
func BroadcastTx(ctx context.Context, serializedTx string) (string, error) {
	if serializedTx == "" {
		return "", ErrInvalidRequest
	}

	tx, err := utils.DecodeBase64Tx(serializedTx)
	if err != nil {
		return "", ErrBadTx
	}

	enc, err := tx.MarshalBinary()
	if err != nil {
		return "", ErrSerializeFailed
	}

	sig, err := Client.SendRawTransaction(ctx, enc)
	if err != nil {
		return "", ErrBroadcastFailed
	}

	return sig.String(), nil
}

// CreateRefundTx 创建退款交易并广播
// orderID: 原收款订单ID
// refundTo: 退款收款人地址
// refundAmount: 退款金额（lamports）
// 返回交易签名和 explorer URL
func CreateRefundTx(ctx context.Context, orderID, refundTo string, refundAmount uint64) (string, string, error) {
	// 验证参数
	if orderID == "" || refundTo == "" || refundAmount == 0 {
		return "", "", ErrInvalidRequest
	}

	// 验证退款收款人地址格式
	refundToPubkey, err := solana.PublicKeyFromBase58(refundTo)
	if err != nil {
		return "", "", ErrInvalidRequest
	}

	// 获取退款账户的 USDC Token Account
	refundPayerPubkey := RefundPayer.PublicKey()
	tokenAccounts, err := Client.GetTokenAccountsByOwner(ctx, refundPayerPubkey, &rpc.GetTokenAccountsConfig{
		Mint: &USDCmint,
	}, &rpc.GetTokenAccountsOpts{
		Encoding: solana.EncodingBase64,
	})
	if err != nil || len(tokenAccounts.Value) == 0 {
		return "", "", errors.New("failed to get refund account's USDC token account")
	}
	sourceTokenAccount := tokenAccounts.Value[0].Pubkey

	// 获取或创建收款人的 USDC Token Account
	var destTokenAccount solana.PublicKey
	destAccounts, err := Client.GetTokenAccountsByOwner(ctx, refundToPubkey, &rpc.GetTokenAccountsConfig{
		Mint: &USDCmint,
	}, &rpc.GetTokenAccountsOpts{
		Encoding: solana.EncodingBase64,
	})
	if err != nil || len(destAccounts.Value) == 0 {
		// 如果没有 Token Account，需要创建（这里简化处理，实际可能需要关联账户）
		return "", "", errors.New("recipient does not have USDC token account")
	}
	destTokenAccount = destAccounts.Value[0].Pubkey

	// 获取最新 blockhash
	bh, err := Client.GetLatestBlockhash(ctx, rpc.CommitmentConfirmed)
	if err != nil {
		return "", "", errors.New("failed to get latest blockhash")
	}

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
		{PublicKey: destTokenAccount, IsSigner: false, IsWritable: true},    // Destination
		{PublicKey: refundPayerPubkey, IsSigner: true, IsWritable: false},  // Owner (authority)
	}
	
	// 创建指令
	instruction := solana.NewInstruction(
		tokenProgramID,
		accounts,
		transferData,
	)

	// 创建交易
	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		bh.Value.Blockhash,
		solana.TransactionPayer(refundPayerPubkey),
	)
	if err != nil {
		return "", "", errors.New("failed to create transaction")
	}

	// 签名交易
	_, err = tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
		if pk.Equals(refundPayerPubkey) {
			return &RefundPayer
		}
		return nil
	})
	if err != nil {
		return "", "", ErrPartialSignFailed
	}

	// 序列化交易
	enc, err := tx.MarshalBinary()
	if err != nil {
		return "", "", ErrSerializeFailed
	}

	// 使用互斥锁保护交易广播
	txMutex.Lock()
	defer txMutex.Unlock()

	// 广播交易（带重试机制）
	var sig solana.Signature
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		sig, err = Client.SendRawTransaction(ctx, enc)
		if err == nil {
			break
		}
	}

	if err != nil {
		return "", "", ErrBroadcastFailed
	}

	signature := sig.String()
	explorerURL := "https://explorer.solana.com/tx/" + signature + "?cluster=mainnet"

	return signature, explorerURL, nil
}
