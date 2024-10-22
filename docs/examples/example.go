package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/prequel-dev/plz4"
)

// Demonstrate writing a compressed lz4 frame.
func compress(out io.Writer) error {

	wr := plz4.NewWriter(out, plz4.WithLevel(3))

	// Always close to release resources; defer is added here in case of error.
	// A double close is noop and will cause no issues.
	defer wr.Close()

	// Use the io.Write interface
	if _, err := wr.Write([]byte("How now")); err != nil {
		return err
	}

	// Force a flush
	if err := wr.Flush(); err != nil {
		return err
	}

	// Use the io.ReaderFrom interface
	rd := bytes.NewReader([]byte(" brown cow"))
	if _, err := wr.ReadFrom(rd); err != nil {
		return err
	}

	// Signal to the writer that there is no more data with a Close.
	// Close() will flush any pending blocks and write the trailer
	return wr.Close()
}

// Demonstrate decompressing an lz4 frame.
func decompress(src io.Reader, dst io.Writer) error {

	rd := plz4.NewReader(src)

	// Always close to release resources; defer is added here in case of error.
	// A double close is noop and will cause no issues.
	defer rd.Close()

	// Use the io.WriterTo interface
	if _, err := rd.WriteTo(dst); err != nil {
		return err
	}

	// It is unlikely to get an error on Close() if WriteTo succeeded,
	// but always a good idea to check for errors.
	return rd.Close()
}

func main() {

	var (
		compressedData bytes.Buffer
		decompressData bytes.Buffer
	)

	// Run example compress routine
	if err := compress(&compressedData); err != nil {
		panic(err)
	}

	// Run example decompress routine
	if err := decompress(&compressedData, &decompressData); err != nil {
		panic(err)
	}

	// Output the result
	fmt.Println(decompressData.String())
}
