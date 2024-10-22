package descriptor

type DataBlockSize uint32

const (
	dbsMask = 0x7FFFFFFF
	cmpMask = 0x80000000
	endMark = 0x00000000
)

func (s DataBlockSize) Size() int          { return int(s & dbsMask) }
func (s DataBlockSize) EOF() bool          { return s == endMark }
func (s DataBlockSize) Uncompressed() bool { return (cmpMask & s) != 0 }

func (s *DataBlockSize) SetSize(v int)    { *s = *s&^dbsMask | DataBlockSize(v)&dbsMask }
func (s *DataBlockSize) SetUncompressed() { *s |= cmpMask }
