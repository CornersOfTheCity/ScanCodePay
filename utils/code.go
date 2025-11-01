package utils

import (
	"encoding/base64"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

func DecodeBase64Tx(b64 string) (*solana.Transaction, error) {
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	tx, err := solana.TransactionFromDecoder(bin.NewBinDecoder(data))
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func EncodeBase64Tx(tx *solana.Transaction) (string, error) {
	enc, err := tx.MarshalBinary()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(enc), nil
}
