package iradix

import (
	"bytes"
)

// Iterator is used to iterate over a set of nodes
// in pre-order.
type Iterator struct {
	node  *Node
	stack []edges
}

// SeekPrefix is used to seek the iterator to a given prefix.
func (i *Iterator) SeekPrefix(prefix []byte) {
	// Wipe the stack
	i.stack = nil
	n := i.node
	search := prefix
	for {
		// Check for key exhaustion.
		if len(search) == 0 {
			i.node = n
			return
		}

		// Look for an edge.
		_, n = n.getEdge(search[0])
		if n == nil {
			i.node = nil
			return
		}

		switch {
		case bytes.HasPrefix(search, n.prefix):
			search = search[len(n.prefix):]
		case bytes.HasPrefix(n.prefix, search):
			i.node = n
			return
		default:
			i.node = nil
			return
		}
	}
}

// Next returns the next node in order.
func (i *Iterator) Next() ([]byte, any, bool) {
	// Initialize our stack if needed
	if i.stack == nil && i.node != nil {
		i.stack = []edges{
			{
				edge{node: i.node},
			},
		}
	}

	for len(i.stack) > 0 {
		// Inspect the last element of the stack.
		n := len(i.stack)
		last := i.stack[n-1]
		elem := last[0].node

		// Update the stack.
		if len(last) > 1 {
			i.stack[n-1] = last[1:]
		} else {
			i.stack = i.stack[:n-1]
		}

		// Push the edges onto the frontier.
		if len(elem.edges) > 0 {
			i.stack = append(i.stack, elem.edges)
		}

		// Return the leaf values if any
		if elem.leaf.key != nil {
			return elem.leaf.key, elem.leaf.val, true
		}
	}
	return nil, nil, false
}
