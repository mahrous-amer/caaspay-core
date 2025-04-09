package transport

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// EncryptAES encrypts data using AES in CFB mode.
func EncryptAES(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("EncryptAES: NewCipher error: %w", err)
	}
	ciphertext := make([]byte, aes.BlockSize+len(data))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("EncryptAES: reading IV error: %w", err)
	}
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], data)
	return ciphertext, nil
}

// DecryptAES decrypts AES-encrypted data using CFB mode.
func DecryptAES(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("DecryptAES: NewCipher error: %w", err)
	}
	if len(data) < aes.BlockSize {
		return nil, fmt.Errorf("DecryptAES: data too short")
	}
	iv := data[:aes.BlockSize]
	data = data[aes.BlockSize:]
	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(data, data)
	return data, nil
}
