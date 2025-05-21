package iradix

// ReverseIterator is used to iterate over a set of nodes
// in reverse in-order.
type ReverseIterator[T any] struct {
	i *Iterator[T]

	// expandedParents stores the set of parent nodes whose relevant children have
	// already been pushed into the stack. This can happen during seek or during
	// iteration.
	//
	// Unlike forward iteration we need to recurse into children before we can
	// output the value stored in an internal leaf since all children are greater.
	// We use this to track whether we have already ensured all the children are
	// in the stack.
	expandedParents map[*Node[T]]struct{}
}

// NewReverseIterator returns a new ReverseIterator at a node.
func NewReverseIterator[T any](n *Node[T]) *ReverseIterator[T] {
	return &ReverseIterator[T]{
		i: &Iterator[T]{node: n},
	}
}

// SeekPrefix is used to seek the iterator to a given prefix.
func (ri *ReverseIterator[T]) SeekPrefix(prefix []byte) {
	ri.i.SeekPrefix(prefix)
}

// Previous returns the previous node in reverse order.
func (ri *ReverseIterator[T]) Previous() *T {
	// Initialize our stack if needed.
	if ri.i.stack == nil && ri.i.node != nil {
		ri.i.stack = []edges[T]{
			{
				edge[T]{node: ri.i.node},
			},
		}
	}

	if ri.expandedParents == nil {
		ri.expandedParents = map[*Node[T]]struct{}{}
	}

	for len(ri.i.stack) > 0 {
		// Inspect the last element of the stack.
		n := len(ri.i.stack)
		last := ri.i.stack[n-1]
		m := len(last)
		elem := last[m-1].node

		_, alreadyExpanded := ri.expandedParents[elem]

		// If this is an internal node and we've not seen it already, we need to
		// leave it in the stack so we can return its possible leaf value _after_
		// we've recursed through all its children.
		if len(elem.edges) > 0 && !alreadyExpanded {
			// record that we've seen this node!
			ri.expandedParents[elem] = struct{}{}
			// push child edges onto stack and skip the rest of the loop to recurse
			// into the largest one.
			ri.i.stack = append(ri.i.stack, elem.edges)
			continue
		}

		// Remove the node from the stack.
		if m > 1 {
			ri.i.stack[n-1] = last[:m-1]
		} else {
			ri.i.stack = ri.i.stack[:n-1]
		}
		// We don't need this state any more as it's no longer in the stack so we
		// won't visit it again.
		if alreadyExpanded {
			delete(ri.expandedParents, elem)
		}

		// If this is a leaf, return it.
		if elem.value != nil {
			return elem.value
		}

		// it's not a leaf so keep walking the stack to find the previous leaf.
	}
	return nil
}
