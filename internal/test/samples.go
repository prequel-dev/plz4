package test

import (
	"bytes"
	"compress/bzip2"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io"
	"testing"
)

//go:embed samples/*
var content embed.FS

const (
	LargeUncompressed = iota
	Uncompressable
	Monster
	Dict
	Lz4_4MB
	Lz4_BlockCRC
	Lz4_ContentCRC
	Lz4_64KB
	Lz4_NoContentCRC
	Lz4_NoBlockCRC
	Lz4_IndieWithDict
	Lz4_Linked
	Lz4_LinkedWithDict
)

var (
	cacheLargeBinary             = bunzip("samples/webster.bz2")
	cacheLargeSha2               = "6a68f69b26daf09f9dd84f7470368553194a0b294fcfa80f1604efb11143a383"
	cacheLz4_4MB_BX_B7           = readAll("samples/dickens.lz4")
	cacheLz4_4MB_BX_B7_Sha2      = "b24c37886142e11d0ee687db6ab06f936207aa7f2ea1fd1d9a36763c7a507e6a"
	cacheLz4_64KB_NOCRC          = readAll("samples/mr.lz4")
	cacheLz4_64KB_NOCRC_Sha2     = "68637ed52e3e4860174ed2dc0840ac77d5f1a60abbcb13770d5754e3774d53e6"
	cache_dict                   = bunzip("samples/dict.bin.bz2")
	cache_dict_Sha2              = "fb0f084fe0e2fceaa7443efeb8f260857dda231199c419d8f8de2946f91c539c"
	cacheLz4_IndieWithDict       = readAll("samples/dickens_dict.lz4")
	cacheLz4_IndieWithDict_Sha2  = "b24c37886142e11d0ee687db6ab06f936207aa7f2ea1fd1d9a36763c7a507e6a"
	cacheLz4_Linked              = readAll("samples/dickens_linked.lz4")
	cacheLz4_Linked_Sha2         = "b24c37886142e11d0ee687db6ab06f936207aa7f2ea1fd1d9a36763c7a507e6a"
	cacheLz4_LinkedWithDict      = readAll("samples/dickens_linked_dict.lz4")
	cacheLz4_LinkedWithDict_Sha2 = "b24c37886142e11d0ee687db6ab06f936207aa7f2ea1fd1d9a36763c7a507e6a"
)

// Various samples for testing different use cases
func LoadSample(t testing.TB, ty int) ([]byte, string) {

	switch ty {
	case LargeUncompressed:
		return cacheLargeBinary, cacheLargeSha2
	case Uncompressable:
		return genUncompressable()
	case Monster:
		return genMonster()
	case Lz4_4MB, Lz4_BlockCRC, Lz4_ContentCRC:
		return cacheLz4_4MB_BX_B7, cacheLz4_4MB_BX_B7_Sha2
	case Lz4_64KB, Lz4_NoContentCRC, Lz4_NoBlockCRC:
		return cacheLz4_64KB_NOCRC, cacheLz4_64KB_NOCRC_Sha2
	case Dict:
		return cache_dict, cache_dict_Sha2
	case Lz4_IndieWithDict:
		return cacheLz4_IndieWithDict, cacheLz4_IndieWithDict_Sha2
	case Lz4_Linked:
		return cacheLz4_Linked, cacheLz4_Linked_Sha2
	case Lz4_LinkedWithDict:
		return cacheLz4_LinkedWithDict, cacheLz4_LinkedWithDict_Sha2
	}

	t.Fatalf("Cannot find sample")
	return nil, ""
}

// Return copy of the sample to allow manipulation without corruption.

func DupeSample(t testing.TB, ty int) ([]byte, string) {
	data, sha2 := LoadSample(t, ty)
	nData := make([]byte, len(data))
	copy(nData, data)
	return nData, sha2
}

func readAll(name string) []byte {
	fh, err := content.Open(name)
	if err != nil {
		panic(err)
	}
	defer fh.Close()
	data, err := io.ReadAll(fh)
	if err != nil {
		panic(err)
	}
	return data
}

func bunzip(name string) []byte {
	data := readAll(name)
	rd := bzip2.NewReader(bytes.NewReader(data))
	data, err := io.ReadAll(rd)
	if err != nil {
		panic(err)
	}
	return data
}

func genUncompressable() ([]byte, string) {

	data := make([]byte, 10<<20)

	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}

	return data, Sha2sum(data)
}

func genMonster() ([]byte, string) {

	targetSize := 2 << 30
	data := make([]byte, 0, targetSize)

	for len(data) < targetSize {

		sz := len(cacheLargeBinary)

		if len(data)+sz > targetSize {
			sz = targetSize - len(data)
		}

		data = append(data, cacheLargeBinary[:sz]...)
	}

	return data, Sha2sum(data)
}

func Sha2sum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
