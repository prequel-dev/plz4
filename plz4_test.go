package plz4

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

func ExampleNewReader() {

	// LZ4 compressed frame containing the payload "hello"
	lz4Data := []byte{0x04, 0x22, 0x4d, 0x18, 0x60, 0x70, 0x73, 0x06, 0x00, 0x00, 0x00, 0x50, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00}

	// Create the Reader with an option
	rd := NewReader(
		bytes.NewReader(lz4Data), // Wrap the src in a reader
		WithParallel(0),          // Run lz4 in synchronous mode
	)

	// Always close; double close is ok.
	defer rd.Close()

	// Use the io.WriterTo interface to decompress the src
	var dst bytes.Buffer
	if _, err := rd.WriteTo(&dst); err != nil {
		panic(err)
	}

	// Close the Reader and check for error.
	// Reader must always be closed to release resources.
	// It is ok to Close more than once, as is done in 'defer Close()' above.
	if err := rd.Close(); err != nil {
		panic(err)
	}

	fmt.Println(dst.String())
	// Output:
	// hello
}

func ExampleNewWriter() {

	var dst bytes.Buffer

	// Create the Writer with two options
	wr := NewWriter(
		&dst,
		WithParallel(1),            // Run in asynchronous mode
		WithContentChecksum(false), // Disable content checksum
	)

	// Always close; double close is ok.
	defer wr.Close()

	// Write source data to be compressed
	if _, err := wr.Write([]byte("hello")); err != nil {
		panic(err)
	}

	// Flush is not required but is shown here as an example
	if err := wr.Flush(); err != nil {
		panic(err)
	}

	// Terminate the LZ4 frame with Close() and check for error
	// Writer must always be closed to release resources.
	// It is ok to Close more than once, as is done in 'defer Close()' above.
	if err := wr.Close(); err != nil {
		panic(err)
	}

	fmt.Println(hex.EncodeToString(dst.Bytes()))
	// Output:
	// 04224d18607073060000005068656c6c6f00000000
}
