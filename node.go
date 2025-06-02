package iradix

import (
	"bytes"
)

// edge is used to represent an edge node.
type edge[T any] struct {
	label byte
	node  *Node[T]
}

// Node is an immutable node in the radix tree.
type Node[T any] struct {
	revision uint64

	value *T

	// prefix is the common prefix we ignore.
	prefix []byte

	// Edges should be stored in-order for iteration.
	// We avoid a fully materialized slice to save memory,
	// since in most cases we expect to be sparse.
	edges edges[T]
}

// Get traverses nodes to find the value of key.
func (n *Node[T]) Get(k []byte) *T {
	search := k
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			return n.value
		}

		// Look for an edge.
		_, n = n.getEdge(search[0])
		if n == nil {
			return nil
		}

		// Consume the search prefix.
		if !bytes.HasPrefix(search, n.prefix) {
			return nil
		}

		search = search[len(n.prefix):]
	}
}

// Iterator is used to return an iterator at
// the given node to walk the tree.
func (n *Node[T]) Iterator() *Iterator[T] {
	return &Iterator[T]{node: n}
}

func (n *Node[T]) addEdge(e edge[T]) {
	num := len(n.edges)
	idx := search[T](n.edges, e.label)
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

func (n *Node[T]) replaceEdge(e edge[T]) {
	num := len(n.edges)
	idx := search[T](n.edges, e.label)
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *Node[T]) getEdge(label byte) (int, *Node[T]) {
	num := len(n.edges)
	idx := search(n.edges, label)
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node[T]) delEdge(label byte) {
	num := len(n.edges)
	idx := search(n.edges, label)
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge[T]{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

func (n *Node[T]) getLowerBoundEdge(label byte) (int, *Node[T]) {
	idx := search(n.edges, label)
	// we want lower bound behavior so return even if it's not an exact match
	if idx < len(n.edges) {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

// rawIterator is used to return a raw iterator at the given node to walk the
// tree.
func search[T any](es edges[T], label byte) int {
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
