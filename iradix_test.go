package iradix

import (
	"crypto/rand"
	"encoding/hex"
	"reflect"
	"sort"
	"testing"
)

func CopyTree(t *Node) *Node {
	nn := &Node{
		revision: t.revision,
		leaf:     t.leaf,
	}
	if t.prefix != nil {
		nn.prefix = make([]byte, len(t.prefix))
		copy(nn.prefix, t.prefix)
	}
	if len(t.edges) != 0 {
		nn.edges = make([]edge, len(t.edges))
		for idx, edge := range t.edges {
			nn.edges[idx].label = edge.label
			nn.edges[idx].node = CopyTree(edge.node)
		}
	}
	return nn
}

func TestRadix_HugeTxn(t *testing.T) {
	r := New()

	// Insert way more nodes than the cache can fit
	txn1 := NewTxn(r)
	var expect []string
	for i := range 800_000 {
		key := randomString(t)
		txn1.Insert([]byte(key), i)
		expect = append(expect, key)
	}
	r = txn1.Commit()
	sort.Strings(expect)

	// Collect the output, should be sorted
	var out []string
	it := r.Iterator()
	for {
		k, _, ok := it.Next()
		if !ok {
			break
		}
		out = append(out, string(k))
	}

	// Verify the match
	if len(out) != len(expect) {
		t.Fatalf("length mis-match: %d vs %d", len(out), len(expect))
	}
	for i, o := range out {
		if o != expect[i] {
			t.Fatalf("mis-match: %v %v", o, expect[i])
		}
	}
}

func TestRadix(t *testing.T) {
	var minValue, maxValue string
	inp := make(map[string]any)
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

	r := New()
	rCopy := CopyTree(r)
	for k, v := range inp {
		txn := NewTxn(r)
		txn.Insert([]byte(k), v)
		newR := txn.Commit()
		if !reflect.DeepEqual(r, rCopy) {
			t.Errorf("r: %#v rc: %#v", r, rCopy)
			t.Errorf("r: %#v rc: %#v", r, rCopy)
		}
		r = newR
		rCopy = CopyTree(r)
	}

	for k, v := range inp {
		out, ok := NewTxn(r).Get([]byte(k))
		if !ok {
			t.Fatalf("missing key: %v", k)
		}
		if out != v {
			t.Fatalf("value mis-match: %v %v", out, v)
		}
	}

	// Copy the full tree before delete
	orig := r
	origCopy := CopyTree(r)

	for k := range inp {
		txn := NewTxn(r)
		txn.Delete([]byte(k))
		r = txn.Commit()
	}

	if !reflect.DeepEqual(orig, origCopy) {
		t.Fatalf("structure modified")
	}
}

func TestRoot(t *testing.T) {
	r := New()
	txn := NewTxn(r)
	txn.Delete(nil)
	r = txn.Commit()
	txn = NewTxn(r)
	txn.Insert(nil, true)
	r = txn.Commit()
	txn = NewTxn(r)
	val, ok := txn.Get(nil)
	if !ok || val != true {
		t.Fatalf("bad: %#v", val)
	}
	r = txn.Commit()
	txn = NewTxn(r)
	txn.Delete(nil)
	txn.Commit()
	val, ok = txn.Get(nil)
	if ok {
		t.Fatalf("bad: %#v", val)
	}
	txn.Commit()
}

func TestDelete(t *testing.T) {
	r := New()
	s := []string{"", "A", "AB"}

	for _, ss := range s {
		txn := NewTxn(r)
		txn.Insert([]byte(ss), true)
		r = txn.Commit()
	}

	for _, ss := range s {
		txn := NewTxn(r)

		v, ok := txn.Get([]byte(ss))
		if !ok || v != true {
			t.Fatalf("bad %q", ss)
		}

		txn.Delete([]byte(ss))

		v, ok = txn.Get([]byte(ss))
		if ok || v == true {
			t.Fatalf("bad %q", ss)
		}

		r = txn.Commit()
	}
}

func TestIteratePrefix(t *testing.T) {
	r := New()

	keys := []string{
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"foobar",
		"zipzap",
	}
	for _, k := range keys {
		txn := NewTxn(r)
		txn.Insert([]byte(k), nil)
		r = txn.Commit()
	}

	type exp struct {
		inp string
		out []string
	}
	cases := []exp{
		{
			"",
			keys,
		},
		{
			"f",
			[]string{
				"foo/bar/baz",
				"foo/baz/bar",
				"foo/zip/zap",
				"foobar",
			},
		},
		{
			"foo",
			[]string{
				"foo/bar/baz",
				"foo/baz/bar",
				"foo/zip/zap",
				"foobar",
			},
		},
		{
			"foob",
			[]string{"foobar"},
		},
		{
			"foo/",
			[]string{"foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		{
			"foo/b",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		{
			"foo/ba",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		{
			"foo/bar",
			[]string{"foo/bar/baz"},
		},
		{
			"foo/bar/baz",
			[]string{"foo/bar/baz"},
		},
		{
			"foo/bar/bazoo",
			[]string{},
		},
		{
			"z",
			[]string{"zipzap"},
		},
	}

	for idx, test := range cases {
		iter := r.Iterator()
		if test.inp != "" {
			iter.SeekPrefix([]byte(test.inp))
		}

		// Consume all the keys
		out := []string{}
		for {
			key, _, ok := iter.Next()
			if !ok {
				break
			}
			out = append(out, string(key))
		}
		if !reflect.DeepEqual(out, test.out) {
			t.Fatalf("mis-match: %d %v %v", idx, out, test.out)
		}
	}
}

func TestMergeChildNilEdges(t *testing.T) {
	r := New()
	txn := NewTxn(r)
	txn.Insert([]byte("foobar"), 42)
	txn.Insert([]byte("foozip"), 43)
	txn.Delete([]byte("foobar"))
	r = txn.Commit()

	out := []string{}
	it := r.Iterator()
	for {
		k, _, ok := it.Next()
		if !ok {
			break
		}
		out = append(out, string(k))
	}

	expect := []string{"foozip"}
	sort.Strings(out)
	sort.Strings(expect)
	if !reflect.DeepEqual(out, expect) {
		t.Fatalf("mis-match: %v %v", out, expect)
	}
}

func TestMergeChildVisibility(t *testing.T) {
	r := New()
	txn := NewTxn(r)
	txn.Insert([]byte("foobar"), 42)
	txn.Insert([]byte("foobaz"), 43)
	txn.Insert([]byte("foozip"), 10)
	r = txn.Commit()

	txn1 := NewTxn(r)
	txn2 := NewTxn(r)

	// Ensure we get the expected value foobar and foobaz
	if val, ok := txn1.Get([]byte("foobar")); !ok || val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := txn1.Get([]byte("foobaz")); !ok || val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := txn2.Get([]byte("foobar")); !ok || val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := txn2.Get([]byte("foobaz")); !ok || val != 43 {
		t.Fatalf("bad: %v", val)
	}

	// Delete of foozip will cause a merge child between the
	// "foo" and "ba" nodes.
	txn2.Delete([]byte("foozip"))

	// Insert of "foobaz" will update the slice of the "fooba" node
	// in-place to point to the new "foobaz" node. This in-place update
	// will cause the visibility of the update to leak into txn1 (prior
	// to the fix).
	txn2.Insert([]byte("foobaz"), 44)

	// Ensure we get the expected value foobar and foobaz
	if val, ok := txn1.Get([]byte("foobar")); !ok || val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := txn1.Get([]byte("foobaz")); !ok || val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := txn2.Get([]byte("foobar")); !ok || val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := txn2.Get([]byte("foobaz")); !ok || val != 44 {
		t.Fatalf("bad: %v", val)
	}

	// Commit txn2
	r = txn2.Commit()

	// Ensure we get the expected value foobar and foobaz
	if val, ok := txn1.Get([]byte("foobar")); !ok || val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := txn1.Get([]byte("foobaz")); !ok || val != 43 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := NewTxn(r).Get([]byte("foobar")); !ok || val != 42 {
		t.Fatalf("bad: %v", val)
	}
	if val, ok := NewTxn(r).Get([]byte("foobaz")); !ok || val != 44 {
		t.Fatalf("bad: %v", val)
	}
}

func TestClone(t *testing.T) {
	r := New()

	t1 := NewTxn(r)
	t1.Insert([]byte("foo"), 7)
	t2 := t1.Clone()

	t1.Insert([]byte("bar"), 42)
	t2.Insert([]byte("baz"), 43)

	if val, ok := t1.Get([]byte("foo")); !ok || val != 7 {
		t.Fatalf("bad foo in t1")
	}
	if val, ok := t2.Get([]byte("foo")); !ok || val != 7 {
		t.Fatalf("bad foo in t2")
	}
	if val, ok := t1.Get([]byte("bar")); !ok || val != 42 {
		t.Fatalf("bad bar in t1")
	}
	if _, ok := t2.Get([]byte("bar")); ok {
		t.Fatalf("bar found in t2")
	}
	if _, ok := t1.Get([]byte("baz")); ok {
		t.Fatalf("baz found in t1")
	}
	if val, ok := t2.Get([]byte("baz")); !ok || val != 43 {
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
