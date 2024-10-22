package compress

const lz4DictSz = 64 << 10

type DictT struct {
	dict   []byte
	linked bool
}

func NewDictT(data []byte, linked bool) *DictT {

	return &DictT{
		dict:   dupeDict(data),
		linked: linked,
	}
}

func (r *DictT) Data() []byte {
	return r.dict
}

func (r *DictT) NeedsUpdate() bool {
	return r.linked
}

// Used in block linked mode

func (r *DictT) Update(dstPtr []byte) {
	switch {
	case len(dstPtr) >= lz4DictSz:
		r.dict = r.dict[:lz4DictSz]
		copy(r.dict, dstPtr[len(dstPtr)-lz4DictSz:])
	case len(r.dict)+len(dstPtr) > lz4DictSz:
		extra := len(r.dict) + len(dstPtr) - lz4DictSz
		copy(r.dict, r.dict[extra:])
		r.dict = r.dict[:len(r.dict)-extra]
		fallthrough
	default:
		r.dict = append(r.dict, dstPtr...)
	}
}

func dupeDict(data []byte) []byte {

	dict := make([]byte, 0, lz4DictSz)

	if len(data) <= lz4DictSz {
		dict = append(dict, data...)
	} else {
		dict = dict[:lz4DictSz]
		extra := len(data) - lz4DictSz
		copy(dict, data[extra:])
	}

	return dict
}
