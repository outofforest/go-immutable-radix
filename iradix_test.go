package iradix

import (
	"crypto/rand"
	"encoding/hex"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func CopyTree[T any](t *Node[T]) *Node[T] {
	nn := &Node[T]{
		revision: t.revision,
		value:    t.value,
	}
	if t.prefix != nil {
		nn.prefix = make([]byte, len(t.prefix))
		copy(nn.prefix, t.prefix)
	}
	if len(t.edges) != 0 {
		nn.edges = make([]edge[T], len(t.edges))
		for idx, edge := range t.edges {
			nn.edges[idx].label = edge.label
			nn.edges[idx].node = CopyTree(edge.node)
		}
	}
	return nn
}

func TestRadix_HugeTxn(t *testing.T) {
	r := New[int]()

	// Insert way more nodes than the cache can fit
	txn1 := NewTxn(r)

	type kv struct {
		k string
		v int
	}

	var pairs []kv
	for i := range 800_000 {
		pair := kv{
			k: randomString(t),
			v: i,
		}
		pairs = append(pairs, pair)
		oldV := txn1.Insert([]byte(pair.k), &pair.v)
		require.Nil(t, oldV)
	}

	sort.Slice(pairs, func(i, j int) bool {
		return strings.Compare(pairs[i].k, pairs[j].k) < 0
	})

	r = txn1.Commit()

	// Collect the output, should be sorted
	var out []int
	it := r.Iterator()
	for {
		v := it.Next()
		if v == nil {
			break
		}
		out = append(out, *v)
	}

	// Verify the match
	if len(out) != len(pairs) {
		t.Fatalf("length mis-match: %d vs %d", len(out), len(pairs))
	}
	for i, o := range out {
		if o != pairs[i].v {
			t.Fatalf("mis-match: %v %v", o, pairs[i].v)
		}
	}
}

func TestRadix(t *testing.T) {
	var minValue, maxValue string
	inp := map[string]int{}
	for i := range 1000 {
		gen := randomString(t)
		inp[gen] = i
		if gen < minValue || i == 0 {
			minValue = gen
		}
		if gen > maxValue || i == 0 {
			maxValue = gen
		}
	}

	r := New[int]()
	rCopy := CopyTree(r)
	for k, v := range inp {
		txn := NewTxn(r)
		oldV := txn.Insert([]byte(k), &v)
		require.Nil(t, oldV)
		newR := txn.Commit()
		if !reflect.DeepEqual(r, rCopy) {
			t.Errorf("r: %#v rc: %#v", r, rCopy)
			t.Errorf("r: %#v rc: %#v", r, rCopy)
		}
		r = newR
		rCopy = CopyTree(r)
	}

	for k, v := range inp {
		out := NewTxn(r).Get([]byte(k))
		if out == nil {
			t.Fatalf("missing key: %v", k)
		}
		if *out != v {
			t.Fatalf("value mis-match: %v %v", out, v)
		}
	}

	// Copy the full tree before delete
	orig := r
	origCopy := CopyTree(r)

	for k, v := range inp {
		txn := NewTxn(r)
		oldV := txn.Delete([]byte(k))
		require.Equal(t, v, *oldV)
		r = txn.Commit()
	}

	if !reflect.DeepEqual(orig, origCopy) {
		t.Fatalf("structure modified")
	}
}

func TestRoot(t *testing.T) {
	r := New[bool]()
	txn := NewTxn(r)
	oldV := txn.Delete(nil)
	require.Nil(t, oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	oldV = txn.Insert(nil, lo.ToPtr(true))
	require.Nil(t, oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	oldV = txn.Insert(nil, lo.ToPtr(false))
	require.True(t, *oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	oldV = txn.Insert(nil, lo.ToPtr(true))
	require.False(t, *oldV)
	r = txn.Commit()

	txn = NewTxn(r)
	val := txn.Get(nil)
	if val == nil || *val != true {
		t.Fatalf("bad: %#v", val)
	}
	r = txn.Commit()
	txn = NewTxn(r)
	oldV = txn.Delete(nil)
	require.True(t, *oldV)
	txn.Commit()

	val = txn.Get(nil)
	if val != nil {
		t.Fatalf("bad: %#v", val)
	}
	txn.Commit()
}

func TestInsertUpdateDelete(t *testing.T) {
	r := New[bool]()
	s := []string{"", "A", "AB"}

	for _, ss := range s {
		txn := NewTxn(r)
		oldV := txn.Insert([]byte(ss), lo.ToPtr(false))
		require.Nil(t, oldV)
		r = txn.Commit()
	}

	for _, ss := range s {
		txn := NewTxn(r)

		v := txn.Get([]byte(ss))
		if v == nil || *v != false {
			t.Fatalf("bad %q", ss)
		}

		r = txn.Commit()
	}

	for _, ss := range s {
		txn := NewTxn(r)
		oldV := txn.Insert([]byte(ss), lo.ToPtr(true))
		require.False(t, *oldV)
		r = txn.Commit()
	}

	for _, ss := range s {
		txn := NewTxn(r)

		v := txn.Get([]byte(ss))
		if v == nil || *v != true {
			t.Fatalf("bad %q", ss)
		}

		oldV := txn.Delete([]byte(ss))
		require.True(t, *oldV)

		v = txn.Get([]byte(ss))
		if v != nil {
			t.Fatalf("bad %q", ss)
		}

		r = txn.Commit()
	}
}

func findIndex(vs []string, v string) int {
	for i, v2 := range vs {
		if v2 == v {
			return i
		}
	}
	return -1
}

func TestIteratePrefix(t *testing.T) {
	r := New[int]()

	keys := []string{
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"foobar",
		"zipzap",
	}
	values := []int{}
	for i, k := range keys {
		txn := NewTxn(r)
		txn.Insert([]byte(k), &i)
		r = txn.Commit()
		values = append(values, i)
	}

	type exp struct {
		inp string
		out []int
	}
	cases := []exp{
		{
			"",
			values,
		},
		{
			"f",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
				findIndex(keys, "foobar"),
			},
		},
		{
			"foo",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
				findIndex(keys, "foobar"),
			},
		},
		{
			"foob",
			[]int{
				findIndex(keys, "foobar"),
			},
		},
		{
			"foo/",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
				findIndex(keys, "foo/zip/zap"),
			},
		},
		{
			"foo/b",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
			},
		},
		{
			"foo/ba",
			[]int{
				findIndex(keys, "foo/bar/baz"),
				findIndex(keys, "foo/baz/bar"),
			},
		},
		{
			"foo/bar",
			[]int{
				findIndex(keys, "foo/bar/baz"),
			},
		},
		{
			"foo/bar/baz",
			[]int{
				findIndex(keys, "foo/bar/baz"),
			},
		},
		{
			"foo/bar/bazoo",
			[]int{},
		},
		{
			"z",
			[]int{
				findIndex(keys, "zipzap"),
			},
		},
	}

	for idx, test := range cases {
		iter := r.Iterator()
		if test.inp != "" {
			iter.SeekPrefix([]byte(test.inp))
		}

		// Consume all the keys
		out := []int{}
		for {
			v := iter.Next()
			if v == nil {
				break
			}
			out = append(out, *v)
		}
		if !reflect.DeepEqual(out, test.out) {
			t.Fatalf("mis-match: %d %v %v", idx, out, test.out)
		}
	}
}

func TestMergeChildNilEdges(t *testing.T) {
	r := New[int]()
	txn := NewTxn(r)
	txn.Insert([]byte("foobar"), lo.ToPtr(42))
	txn.Insert([]byte("foozip"), lo.ToPtr(43))
	txn.Delete([]byte("foobar"))
	r = txn.Commit()

	out := []int{}
	it := r.Iterator()
	for {
		v := it.Next()
		if v == nil {
			break
		}
		out = append(out, *v)
	}

	expect := []int{43}
	if !reflect.DeepEqual(out, expect) {
		t.Fatalf("mis-match: %v %v", out, expect)
	}
}

func TestMergeChildVisibility(t *testing.T) {
	r := New[int]()
	txn := NewTxn(r)
	txn.Insert([]byte("foobar"), lo.ToPtr(42))
	txn.Insert([]byte("foobaz"), lo.ToPtr(43))
	txn.Insert([]byte("foozip"), lo.ToPtr(10))
	r = txn.Commit()

	txn1 := NewTxn(r)
	txn2 := NewTxn(r)

	// Ensure we get the expected value foobar and foobaz
	if val := txn1.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn1.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}

	// Delete of foozip will cause a merge child between the
	// "foo" and "ba" nodes.
	txn2.Delete([]byte("foozip"))

	// Insert of "foobaz" will update the slice of the "fooba" node
	// in-place to point to the new "foobaz" node. This in-place update
	// will cause the visibility of the update to leak into txn1 (prior
	// to the fix).
	txn2.Insert([]byte("foobaz"), lo.ToPtr(44))

	// Ensure we get the expected value foobar and foobaz
	if val := txn1.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn1.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn2.Get([]byte("foobaz")); val == nil || *val != 44 {
		t.Fatalf("bad: %v", val)
	}

	// Commit txn2
	r = txn2.Commit()

	// Ensure we get the expected value foobar and foobaz
	if val := txn1.Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := txn1.Get([]byte("foobaz")); val == nil || *val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val := NewTxn(r).Get([]byte("foobar")); val == nil || *val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val := NewTxn(r).Get([]byte("foobaz")); val == nil || *val != 44 {
		t.Fatalf("bad: %v", val)
	}
}

func TestClone(t *testing.T) {
	r := New[int]()

	t1 := NewTxn(r)
	t1.Insert([]byte("foo"), lo.ToPtr(7))
	t2 := t1.Clone()

	t1.Insert([]byte("bar"), lo.ToPtr(42))
	t2.Insert([]byte("baz"), lo.ToPtr(43))

	if val := t1.Get([]byte("foo")); val == nil || *val != 7 {
		t.Fatalf("bad foo in t1")
	}
	if val := t2.Get([]byte("foo")); val == nil || *val != 7 {
		t.Fatalf("bad foo in t2")
	}
	if val := t1.Get([]byte("bar")); val == nil || *val != 42 {
		t.Fatalf("bad bar in t1")
	}
	if val := t2.Get([]byte("bar")); val != nil {
		t.Fatalf("bar found in t2")
	}
	if val := t1.Get([]byte("baz")); val != nil {
		t.Fatalf("baz found in t1")
	}
	if val := t2.Get([]byte("baz")); val == nil || *val != 43 {
		t.Fatalf("bad baz in t2")
	}
}

func randomString(t *testing.T) string {
	var gen [16]byte
	_, err := rand.Read(gen[:])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return hex.EncodeToString(gen[:])
}
