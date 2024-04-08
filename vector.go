package negentropy

import (
	"errors"
	"sort"
)

type Vector struct {
	items  []Item
	sealed bool
}

func NewVector() *Vector {
	return &Vector{
		items:  make([]Item, 0),
		sealed: false,
	}
}

func (v *Vector) Insert(createdAt uint64, id []byte) error {
	//fmt.Fprintln(os.Stderr, "Insert", createdAt, id)
	if v.sealed {
		return errors.New("already sealed")
	}
	if len(id) != IDSize {
		return errors.New("bad id size for added item")
	}
	item := NewItemWithID(createdAt, id)

	v.items = append(v.items, *item)
	return nil
}

func (v *Vector) InsertItem(item Item) error {
	return v.Insert(item.Timestamp, item.ID[:])
}

func (v *Vector) Seal() error {
	if v.sealed {
		return errors.New("already sealed")
	}
	v.sealed = true

	sort.Slice(v.items, func(i, j int) bool {
		return v.items[i].LessThan(v.items[j])
	})

	for i := 1; i < len(v.items); i++ {
		if v.items[i-1].Equals(v.items[i]) {
			return errors.New("duplicate item inserted")
		}
	}
	return nil
}

func (v *Vector) Unseal() {
	v.sealed = false
}

func (v *Vector) Size() int {
	if err := v.checkSealed(); err != nil {
		return 0
	}
	return len(v.items)
}

func (v *Vector) GetItem(i uint64) (Item, error) {
	if err := v.checkSealed(); err != nil {
		return Item{}, err
	}
	if i >= uint64(len(v.items)) {
		return Item{}, errors.New("index out of bounds")
	}
	return v.items[i], nil
}

func (v *Vector) Iterate(begin, end int, cb func(Item, int) bool) error {
	if err := v.checkSealed(); err != nil {
		return err
	}
	if err := v.checkBounds(begin, end); err != nil {
		return err
	}

	for i := begin; i < end; i++ {
		if !cb(v.items[i], i) {
			break
		}
	}
	return nil
}

func (v *Vector) FindLowerBound(begin, end int, bound Bound) (int, error) {
	if err := v.checkSealed(); err != nil {
		return 0, err
	}
	if err := v.checkBounds(begin, end); err != nil {
		return 0, err
	}

	i := sort.Search(len(v.items[begin:end]), func(i int) bool {
		return !v.items[begin+i].LessThan(bound.Item)
	})
	return begin + i, nil
}

func (v *Vector) Fingerprint(begin, end int) (Fingerprint, error) {
	var out Accumulator
	out.SetToZero()

	if err := v.Iterate(begin, end, func(item Item, _ int) bool {
		out.AddItem(item)
		return true
	}); err != nil {
		return Fingerprint{}, err
	}

	return out.GetFingerprint(end - begin), nil
}

func (v *Vector) checkSealed() error {
	if !v.sealed {
		return errors.New("not sealed")
	}
	return nil
}

func (v *Vector) checkBounds(begin, end int) error {
	if begin > end || end > len(v.items) {
		return errors.New("bad range")
	}
	return nil
}

