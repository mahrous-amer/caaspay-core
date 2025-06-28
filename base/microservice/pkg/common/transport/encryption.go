package transport

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
)

// EncryptAES encrypts data using AES-GCM with a 12-byte nonce.
func EncryptAES(data []byte, hexKey string) ([]byte, error) {
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("EncryptAES: Failed to decode hex key: %v", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("EncryptAES: NewCipher error: %w Key: %s", err, key)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("EncryptAES: NewGCM error: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("EncryptAES: generating nonce error: %w", err)
	}

	ciphertext := aesGCM.Seal(nil, nonce, data, nil)

	// Prepend nonce to ciphertext
	return append(nonce, ciphertext...), nil
}

// DecryptAES decrypts data encrypted with AES-GCM.
func DecryptAES(data []byte, hexKey string) ([]byte, error) {
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("DecryptAES: Failed to decode hex key: %v", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("DecryptAES: NewCipher error: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("DecryptAES: NewGCM error: %w", err)
	}

	nonceSize := aesGCM.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("DecryptAES: data too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("DecryptAES: open failed: %w", err)
	}

	return plaintext, nil
}
