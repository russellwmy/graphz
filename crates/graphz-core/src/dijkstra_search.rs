use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::types::NodeId;
use crate::Graph;

/// State represents a node in the path-finding process
/// cost: the total cost to reach this node
/// position: the current node's ID
#[derive(Copy, Clone, Eq, PartialEq)]
struct State {
    cost: u32,
    position: NodeId,
}

// Custom ordering for State to create a min-heap based on cost
impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Implements Dijkstra's shortest path algorithm
/// Returns the shortest path from start to end as a vector of node IDs
/// Returns None if no path exists
pub fn dijkstra_search(graph: &Graph, start: NodeId, end: NodeId) -> Option<Vec<NodeId>> {
    // Track shortest distance to each node
    let mut dist: HashMap<NodeId, u32> = HashMap::new();
    // Track previous node in optimal path
    let mut prev: HashMap<NodeId, NodeId> = HashMap::new();
    // Priority queue for nodes to visit
    let mut heap = BinaryHeap::new();

    // Initialize start node
    dist.insert(start, 0);
    heap.push(State { cost: 0, position: start });

    while let Some(State { cost, position }) = heap.pop() {
        // If we reached the end, reconstruct and return the path
        if position == end {
            let mut path = vec![end];
            let mut current = end;
            while let Some(&previous) = prev.get(&current) {
                path.push(previous);
                current = previous;
            }
            path.reverse();
            return Some(path);
        }

        // Skip if we've found a better path
        if cost > dist[&position] {
            continue;
        }

        // Explore neighbors
        for (neighbor, weight) in graph.neighbors_with_weights(position).unwrap() {
            let next = State { cost: cost + weight as u32, position: neighbor };

            // Update if we found a shorter path
            if !dist.contains_key(&next.position) || next.cost < dist[&next.position] {
                heap.push(next);
                dist.insert(next.position, next.cost);
                prev.insert(next.position, position);
            }
        }
    }

    // No path found
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{edge::Edge, Graph};

    #[test]
    fn test_dijkstra_search() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).weight(1.0).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).weight(2.0).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).weight(3.0).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).weight(4.0).build().unwrap(),
            Edge::builder().source_id(5).target_id(6).weight(5.0).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();
        let path = dijkstra_search(&graph, 1, 4);
        assert_eq!(path, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_no_path() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).weight(1.0).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).weight(2.0).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).weight(3.0).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();
        // No path from 1 to 5
        let path = dijkstra_search(&graph, 1, 5);
        assert_eq!(path, None);
    }
}
