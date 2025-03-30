use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
};

use crate::{types::NodeId, Graph};

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

pub fn a_search(graph: &Graph, start: NodeId, end: NodeId) -> Option<Vec<NodeId>> {
    // Priority queue for the open set
    let mut open_set = BinaryHeap::new();
    // Map to track the cost of reaching each node
    let mut g_score = HashMap::new();
    // Map to track the best path to each node
    let mut came_from = HashMap::new();

    // Initialize the starting node
    g_score.insert(start, 0);
    open_set.push(State { cost: 0, position: start });

    while let Some(State { cost: _, position }) = open_set.pop() {
        // If we reached the end, reconstruct and return the path
        if position == end {
            let mut path = vec![end];
            let mut current = end;
            while let Some(&previous) = came_from.get(&current) {
                path.push(previous);
                current = previous;
            }
            path.reverse();
            return Some(path);
        }

        // Explore neighbors
        for (neighbor, weight) in graph.neighbors_with_weights(position).unwrap() {
            let tentative_g_score = g_score[&position] + weight as u32;

            // If this path is better, record it
            if tentative_g_score < *g_score.get(&neighbor).unwrap_or(&u32::MAX) {
                came_from.insert(neighbor, position);
                g_score.insert(neighbor, tentative_g_score);
                open_set.push(State { cost: tentative_g_score, position: neighbor });
            }
        }
    }

    // No path found
    None
}

#[cfg(test)]
mod tests {
    use crate::edge::Edge;

    use super::*;

    #[test]
    fn test_a_search() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).weight(1.0).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).weight(2.0).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).weight(3.0).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).weight(4.0).build().unwrap(),
            Edge::builder().source_id(5).target_id(6).weight(5.0).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();
        let path = a_search(&graph, 1, 4);
        assert_eq!(path, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_a_search_no_path() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).weight(1.0).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).weight(2.0).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).weight(3.0).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();
        // No path from 1 to 5
        let path = a_search(&graph, 1, 5);
        assert_eq!(path, None);
    }
}
