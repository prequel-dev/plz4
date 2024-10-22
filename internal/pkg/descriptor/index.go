package descriptor

type BlockIdxT uint8

const (
	BlockIdx64KB  BlockIdxT = 4
	BlockIdx256KB BlockIdxT = 5
	BlockIdx1MB   BlockIdxT = 6
	BlockIdx4MB   BlockIdxT = 7

	BlockIdx64KBSz  = 64 << 10
	BlockIdx256KBSz = 256 << 10
	BlockIdx1MBSz   = 1 << 20
	BlockIdx4MBSz   = 4 << 20

	blockIdx64KBStr  = "64KiB"
	blockIdx256KBStr = "256KiB"
	blockIdx1MBStr   = "1MiB"
	blockIdx4MBStr   = "4MiB"
)

func (idx BlockIdxT) Valid() bool {
	return idx >= 4 && idx <= 7
}

func (idx BlockIdxT) Size() (v int) {
	switch idx {
	case BlockIdx64KB:
		v = BlockIdx64KBSz
	case BlockIdx256KB:
		v = BlockIdx256KBSz
	case BlockIdx1MB:
		v = BlockIdx1MBSz
	case BlockIdx4MB:
		v = BlockIdx4MBSz
	}
	return
}

func (idx BlockIdxT) Str() (s string) {
	switch idx {
	case BlockIdx64KB:
		s = blockIdx64KBStr
	case BlockIdx256KB:
		s = blockIdx256KBStr
	case BlockIdx1MB:
		s = blockIdx1MBStr
	case BlockIdx4MB:
		s = blockIdx4MBStr
	default:
		s = "undefined"
	}
	return
}
