//go:build !cgo

package test

func init() {
	cgoEnabled = false
}
