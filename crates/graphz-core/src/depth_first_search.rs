use crate::{types::NodeId, Graph};
use std::collections::HashSet;
use tracing::{debug, info};

pub fn depth_first_search(graph: &Graph, start: NodeId, end: NodeId) -> Option<Vec<NodeId>> {
    info!("Starting DFS from node {} to node {}", start, end);
    let mut visited = HashSet::new();
    let mut stack = Vec::new();

    stack.push((start, vec![start]));

    while let Some((current, current_path)) = stack.pop() {
        debug!("Processing node {} (current path: {:?})", current, current_path);

        if current == end {
            info!("Found path: {:?}", current_path);
            return Some(current_path);
        }

        if !visited.contains(&current) {
            debug!("Visiting node {}", current);
            visited.insert(current);

            for idx in 0..graph.num_edges() {
                let source_id = graph.source_id(idx).unwrap();
                let target_id = graph.target_id(idx).unwrap();
                if source_id == current {
                    if !visited.contains(&target_id) {
                        let mut new_path = current_path.clone();
                        new_path.push(target_id);
                        debug!("Adding node {} to stack with path: {:?}", target_id, new_path);
                        stack.push((target_id, new_path));
                    }
                }
            }
        } else {
            debug!("Node {} already visited, skipping", current);
        }
    }

    info!("No path found from {} to {}", start, end);
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{edge::Edge, Graph};

    #[test]
    fn test_bfs() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).build().unwrap(),
            Edge::builder().source_id(5).target_id(6).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();
        let path = depth_first_search(&graph, 1, 3);
        assert_eq!(path, Some(vec![1, 2, 3]));
    }
}
