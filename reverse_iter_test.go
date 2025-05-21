package iradix

import "testing"

func TestReverseIterator_SeekPrefix(t *testing.T) {
	r := New[int]()
	keys := []string{"001", "002", "005", "010", "100"}
	for i, k := range keys {
		txn := NewTxn(r)
		txn.Insert([]byte(k), &i)
		r = txn.Commit()
	}

	cases := []struct {
		name         string
		prefix       string
		expectResult bool
	}{
		{
			name:         "existing prefix",
			prefix:       "005",
			expectResult: true,
		},
		{
			name:         "non-existing prefix",
			prefix:       "2",
			expectResult: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			it := r.ReverseIterator()
			it.SeekPrefix([]byte(c.prefix))

			if c.expectResult && it.i.node == nil {
				t.Errorf("expexted prefix %s to exist", c.prefix)
				return
			}

			if !c.expectResult && it.i.node != nil {
				t.Errorf("unexpected node for prefix '%s'", c.prefix)
				return
			}
		})
	}
}

func TestReverseIterator_Previous(t *testing.T) {
	r := New[int]()
	keys := []string{"001", "002", "005", "010", "100"}
	for i, k := range keys {
		txn := NewTxn(r)
		txn.Insert([]byte(k), &i)
		r = txn.Commit()
	}

	it := r.ReverseIterator()

	for i := len(keys) - 1; i >= 0; i-- {
		got := it.Previous()

		if *got != i {
			t.Errorf("got: %v, want: %v", got, i)
		}
	}
}
