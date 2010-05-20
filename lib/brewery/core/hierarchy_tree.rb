# HierarchyTree - used for building and handling dimension hierarchy
# Note: not optimized for general tree structure storage
#
# Copyright (C) 2010 Knowerce, s.r.o.
# 
# Written by: Stefan Urbanek
# Date: May 2010
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

module Brewery

class HierarchyTreeNode

attr_accessor :represented_object
attr_accessor :children
def initialize
	@children = Hash.new
end

def subtree_depth
	if is_leaf
		return 0
	end
	depths = @children.values.collect { |value| value.subtree_depth }

	return depths.max + 1
end

def is_leaf
	return @children.count == 0
end

def add_child(node_name, node)
	@children[node_name] = node
end

def child(node_name)
	return @children[node_name]
end

def children_names
	return @children.keys
end

def children_nodes
	return @children.values
end

def paths_at_depth(depth, sorted = false)
	if depth == 0
		names = children_names
		if sorted
			names = names.sort
		end
		return names.collect { |name| [name] }
	end

	paths = Array.new
	keys = @children.keys
	if sorted
		keys = keys.sort
	end

	keys.each { |key|
		child = @children[key]
		child_paths = child.paths_at_depth(depth - 1)
		
		child_paths.each { | path |
			paths << [key] + path
		}
	}

	return paths		
end

end # class

class HierarchyTree

def initialize
	@root = HierarchyTreeNode.new
end

# Return lenght of deepest path
def depth
	return @root.subtree_depth
end

# Create path if it does not exist yet.
def add_path(path)
	_create_node_at_path(path)
end

# Set represented object for @path
def set_object_at_path(path, object)
	node = _create_node_at_path(path)
	node.represented_object = object
end

# Return all existing paths with @level length
def paths_at_level(level, sorted = false)
	paths = @root.paths_at_depth(level, sorted)
	return paths
end

# Return represented object at @path
def object_at_path(path)
	if path.count == 0
		return nil
	end

	node = @root

	path.each { |node_name|
		node = node.child(node_name)
	}
	return node ? node.represented_object : nil
end

def each(sorted = false)
	d = depth

	# FIXME: this is very slow, make it recursive
	
	for level in 0..(d-1)
		paths = paths_at_level(level, sorted)
		paths.each { |path|
			object = object_at_path(path)
			yield path, object
		}
	end
end

private
# @private
def _create_node_at_path(path)
	current_node = @root
	
	super_node = nil
	
	path.each { |node_name|
		child = current_node.child(node_name)
		if not child
			new_node = HierarchyTreeNode.new
			current_node.add_child(node_name, new_node)
			current_node = new_node
		else
			current_node = current_node.child(node_name)
		end
	}
	
	return current_node
end

end # class

end # module