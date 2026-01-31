package blk

import (
	"bytes"
	"testing"
)

func TestShrinkToFit(t *testing.T) {
	tests := []struct {
		name           string
		buf            []byte
		n              int
		want           []byte
		sameUnderlying bool
	}{
		{
			name:           "no shrink when n equals len",
			buf:            []byte{1, 2, 3},
			n:              3,
			want:           []byte{1, 2, 3},
			sameUnderlying: true,
		},
		{
			name:           "shrink when n less than len",
			buf:            []byte{1, 2, 3, 4, 5},
			n:              3,
			want:           []byte{1, 2, 3},
			sameUnderlying: false,
		},
		{
			name:           "shrink to zero length",
			buf:            []byte{1, 2, 3},
			n:              0,
			want:           []byte{},
			sameUnderlying: false,
		},
		{
			name:           "n greater than len returns original",
			buf:            []byte{1, 2},
			n:              3,
			want:           []byte{1, 2},
			sameUnderlying: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShrinkToFit(tt.buf, tt.n)

			if !bytes.Equal(got, tt.want) {
				t.Fatalf("unexpected contents: got %v, want %v", got, tt.want)
			}

			if tt.sameUnderlying {
				if len(got) > 0 && len(tt.buf) > 0 && &got[0] != &tt.buf[0] {
					t.Fatalf("expected result to share underlying array with input")
				}
			} else {
				if len(got) > 0 && len(tt.buf) > 0 && &got[0] == &tt.buf[0] {
					t.Fatalf("expected result to have new underlying array")
				}
				if cap(got) != len(got) {
					t.Fatalf("expected shrunk slice to have cap == len, got len=%d cap=%d", len(got), cap(got))
				}
			}
		})
	}
}
