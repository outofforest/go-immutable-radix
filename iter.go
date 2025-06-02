package iradix

import (
	"bytes"
)

// Iterator is used to iterate over a set of nodes
// in pre-order.
type Iterator[T any] struct {
	node  *Node[T]
	stack []edges[T]
	skip  int
}

// SeekPrefix is used to seek the iterator to a given prefix.
func (i *Iterator[T]) SeekPrefix(prefix []byte) {
	// Wipe the stack
	i.stack = nil
	search := prefix
	for {
		// Check for key exhaustion.
		if len(search) == 0 {
			i.skip = len(i.node.prefix)
			return
		}

		// Look for an edge.
		_, i.node = i.node.getEdge(search[0])
		switch {
		case i.node == nil:
			return
		case bytes.HasPrefix(search, i.node.prefix):
			search = search[len(i.node.prefix):]
		case bytes.HasPrefix(i.node.prefix, search):
			i.skip = len(search)
			return
		default:
			i.node = nil
			return
		}
	}
}

// SeekLowerBound is used to seek the iterator to the smallest key that is
// greater or equal to the given key. There is no watch variant as it's hard to
// predict based on the radix structure which node(s) changes might affect the
// result.
func (i *Iterator[T]) SeekLowerBound(key []byte) {
	if i.node == nil {
		return
	}
	// Wipe the stack. Unlike Prefix iteration, we need to build the stack as we
	// go because we need only a subset of edges of many nodes in the path to the
	// leaf with the lower bound. Note that the iterator will still recurse into
	// children that we don't traverse on the way to the reverse lower bound as it
	// walks the stack.
	i.stack = []edges[T]{}
	// i.node starts off in the common case as pointing to the root node of the
	// tree. By the time we return we have either found a lower bound and setup
	// the stack to traverse all larger keys, or we have not and the stack and
	// node should both be nil to prevent the iterator from assuming it is just
	// iterating the whole tree from the root node. Either way this needs to end
	// up as nil so just set it here.
	n := i.node
	i.node = nil
	search := key

	found := func(n *Node[T]) {
		i.stack = append(i.stack, edges[T]{edge[T]{node: n}})
	}

	findMin := func(n *Node[T]) {
		if n := i.findMin(n); n != nil {
			i.stack = append(i.stack, edges[T]{edge[T]{node: n}})
		}
	}

	for {
		prefix := n.prefix
		if i.skip > 0 {
			prefix = prefix[i.skip:]
			i.skip = 0
		}

		// Compare current prefix with the search key's same-length prefix.
		var prefixCmp int
		if len(prefix) < len(search) {
			prefixCmp = bytes.Compare(prefix, search[:len(prefix)])
		} else {
			prefixCmp = bytes.Compare(prefix, search)
		}

		if prefixCmp > 0 {
			// Prefix is larger, that means the lower bound is greater than the search
			// and from now on we need to follow the minimum path to the smallest
			// leaf under this subtree.
			findMin(n)
			return
		}

		if prefixCmp < 0 {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			return
		}

		// Prefix is equal, we are still heading for an exact match. If this is a
		// leaf and an exact match we're done.
		if len(prefix) == len(search) && n.value != nil {
			found(n)
			return
		}

		// Consume the search prefix if the current node has one. Note that this is
		// safe because if n.prefix is longer than the search slice prefixCmp would
		// have been > 0 above and the method would have already returned.
		search = search[len(prefix):]

		if len(search) == 0 {
			// We've exhausted the search key, but the current node is not an exact
			// match or not a leaf. That means that the leaf value if it exists, and
			// all child nodes must be strictly greater, the smallest key in this
			// subtree must be the lower bound.
			findMin(n)
			return
		}

		// Otherwise, take the lower bound next edge.
		idx, lbNode := n.getLowerBoundEdge(search[0])
		if lbNode == nil {
			return
		}

		// Create stack edges for the all strictly higher edges in this node.
		if idx+1 < len(n.edges) {
			i.stack = append(i.stack, n.edges[idx+1:])
		}

		// Recurse
		n = lbNode
	}
}

// Next returns the next node in order.
func (i *Iterator[T]) Next() *T {
	// Initialize our stack if needed
	if i.stack == nil && i.node != nil {
		i.stack = []edges[T]{
			{
				edge[T]{node: i.node},
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
		if elem.value != nil {
			return elem.value
		}
	}
	return nil
}

func (i *Iterator[T]) findMin(n *Node[T]) *Node[T] {
	// Traverse to the minimum child
	for {
		if n.value != nil {
			return n
		}
		nEdges := len(n.edges)
		if nEdges > 1 {
			// Add all the other edges to the stack (the min node will be added as
			// we recurse)
			i.stack = append(i.stack, n.edges[1:])
		}
		n = n.edges[0].node
	}
}
