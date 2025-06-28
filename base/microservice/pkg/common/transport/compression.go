package transport

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// CompressZstd compresses data using zstd.
func CompressZstd(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("CompressZstd: NewWriter error: %w", err)
	}
	defer encoder.Close()
	compressed := encoder.EncodeAll(data, nil)
	return compressed, nil
}

// DecompressZstd decompresses zstd-compressed data.
func DecompressZstd(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("DecompressZstd: NewReader error: %w", err)
	}
	defer decoder.Close()
	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("DecompressZstd: DecodeAll error: %w", err)
	}
	return decompressed, nil
}
