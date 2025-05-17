package iradix

import "bytes"

// WalkFn is used when walking the tree. Takes a
// key and value, returning if iteration should
// be terminated.
type WalkFn func(k []byte, v any) bool

// leafNode is used to represent a value.
type leafNode struct {
	key []byte
	val any
}

// edge is used to represent an edge node.
type edge struct {
	label byte
	node  *Node
}

// Node is an immutable node in the radix tree.
type Node struct {
	revision uint64

	// leaf is used to store possible leaf.
	leaf leafNode

	// prefix is the common prefix we ignore.
	prefix []byte

	// Edges should be stored in-order for iteration.
	// We avoid a fully materialized slice to save memory,
	// since in most cases we expect to be sparse.
	edges edges
}

// Get traverses nodes to find the value of key.
func (n *Node) Get(k []byte) (any, bool) {
	search := k
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf.val, true
			}
			break
		}

		// Look for an edge.
		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix.
		if !bytes.HasPrefix(search, n.prefix) {
			break
		}

		search = search[len(n.prefix):]
	}
	return nil, false
}

// Iterator is used to return an iterator at
// the given node to walk the tree.
func (n *Node) Iterator() *Iterator {
	return &Iterator{node: n}
}

// ReverseIterator is used to return an iterator at
// the given node to walk the tree backwards.
func (n *Node) ReverseIterator() *ReverseIterator {
	return NewReverseIterator(n)
}

func (n *Node) isLeaf() bool {
	return n.leaf.key != nil
}

func (n *Node) addEdge(e edge) {
	num := len(n.edges)
	idx := search(n.edges, e.label)
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

func (n *Node) replaceEdge(e edge) {
	num := len(n.edges)
	idx := search(n.edges, e.label)
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *Node) getEdge(label byte) (int, *Node) {
	num := len(n.edges)
	idx := search(n.edges, label)
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node) delEdge(label byte) {
	num := len(n.edges)
	idx := search(n.edges, label)
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

// rawIterator is used to return a raw iterator at the given node to walk the
// tree.
func search(es edges, label byte) int {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, len(es)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h.
		// i ≤ h < j
		if es[h].label < label {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i
}
