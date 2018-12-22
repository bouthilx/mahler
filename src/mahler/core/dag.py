

class DAGNode(object):
    """
    """

    __slots__ = ('_item', '_parents', '_children')

    def __init__(self, item, parents=tuple(), children=tuple()):
        """Initialize node with item, parents and children

        .. seealso::
            :class:`laboratorium.engine.dag.DAGNode` for information about the attributes
        """
        self._item = item
        self._parents = []
        self._children = []

        if parents:
            self.add_parents(*parents)
        if children:
            self.add_children(*children)

    @property
    def item(self):
        """Get item of the node which may contain arbitrary objects"""
        return self._item

    @item.setter
    def item(self, new_item):
        """Set item of the node with arbitrary objects"""
        self._item = new_item

    @property
    def parent(self):
        """Get parent of the node, None if no parent"""
        return self._parent

    def drop_parents(self, *nodes):
        """Drop the parent of the node, do nothing if no parent

        Note that the node will be removed from the children of the parent as well
        """
        if not nodes:
            nodes = self.parents

        for parent in list(nodes):
            parent.drop_children(self)

    # TODO: Detect cycles
    def add_parents(self, *nodes):
        """Add parents to the node

        .. seealso::
            `laboratorium.engine.dag.DAGNode.drop_parents`
        """
        for parent in nodes:
            if parent is not None and not isinstance(parent, DAGNode):
                raise TypeError("Cannot add %s to parents" % str(parent))

            if parent not in self._parents:
                parent.add_children(self)

    @property
    def children(self):
        """Get children of the node, empty list if no children"""
        return self._children

    def drop_children(self, *nodes):
        """Drop the children of the node, do nothing if no parent

        If no nodes are passed, the method will drop all the children of the current node.

        Note that the parent of the given node will be removed as well

        Raises
        ------
        ValueError
            If one of the given nodes is not a children of the current node.

        """
        if not nodes:
            nodes = self.children

        for child in list(nodes):
            del self._children[self.children.index(child)]
            # pylint: disable=protected-access
            del child._parents[child.parents.index(self)]

    def add_children(self, *nodes):
        """Add children to the current node

        Note that added children will have their parent set to the current node as well.

        .. seealso::
            `orion.core.evc.tree.TreeNode.drop_children`
        """
        for child in nodes:
            if child is not None and not isinstance(child, DAGNode):
                raise TypeError("Cannot add %s to children" % str(child))

            if child not in self._children:
                # TreeNode.set_parent uses add_children so using it here could cause an infinit
                # recursion. add_children() gets the dirty job done.
                self._children.append(child)
                # pylint: disable=protected-access
                child._parents.append(self)

    # TODO
    # def __iter__(self):
    #     """Iterate on the tree with pre-order traversal"""
    #     return PreOrderTraversal(self)

    def __repr__(self):
        """Represent the object as a string."""
        parents = [parent.item for parent in self.parents]
        children = [child.item for child in self.children]

        return ("%s(%s, parents=%s, children=%s)" %
                (self.__class__.__name__, str(self.item), str(parents), str(children)))
