package main

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// deriveKeyFromPassphrase hashes the passphrase to a 32-byte AES-256 key
func deriveKeyFromPassphrase(passphrase string) []byte {
	hash := sha256.Sum256([]byte(passphrase))
	return hash[:] // 32 bytes = AES-256
}

func main() {
	// Generate secure random AES-256 key
	randomKey := make([]byte, 32)
	if _, err := rand.Read(randomKey); err != nil {
		panic("❌ Failed to generate random key: " + err.Error())
	}

	fmt.Println("🔐 Random AES-256 Key (hex-encoded):")
	fmt.Printf("  %x\n", randomKey)
	fmt.Println("  Use this for ephemeral or config-based encryption keys.\n")

	// Prompt user for a passphrase
	fmt.Print("Enter passphrase to derive AES-256 key: ")
	reader := bufio.NewReader(os.Stdin)
	passphrase, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("❌ Failed to read input:", err)
		return
	}

	passphrase = strings.TrimSpace(passphrase)
	if len(passphrase) == 0 {
		fmt.Println("⚠️ Passphrase was empty — no key derived.")
		return
	}

	derivedKey := deriveKeyFromPassphrase(passphrase)
	hexKey := hex.EncodeToString(derivedKey)

	fmt.Println("\n🔑 AES-256 Key derived from your passphrase using SHA-256:")
	fmt.Printf("  HEX: %s\n", hexKey)
	fmt.Printf("  KEY: %T\n", derivedKey)
	fmt.Println("  Use this for long-term secure config (e.g. env var or secret file).")
}
