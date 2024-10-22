package blk

type BlkRdrI interface {
	NextBlock(*BlkT) (*BlkT, int, error)
	Close()
}
