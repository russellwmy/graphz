use crate::{types::NodeId, Graph};
use std::collections::{HashSet, VecDeque};
use tracing::{debug, info};

pub fn breath_first_search(graph: &Graph, start: NodeId, end: NodeId) -> Option<Vec<NodeId>> {
    info!("Starting BFS from node {} to node {}", start, end);
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back((start, vec![start]));

    while let Some((current, current_path)) = queue.pop_front() {
        debug!("Processing node {} (current path: {:?})", current, current_path);

        if current == end {
            info!("Found path: {:?}", current_path);
            return Some(current_path);
        }

        if !visited.insert(current) {
            debug!("Node {} already visited, skipping", current);
            continue;
        }
        for idx in 0..graph.num_edges() {
            if graph.source_id(idx).unwrap() == current {
                let mut new_path = current_path.clone();
                new_path.push(graph.target_id(idx).unwrap());
                info!("Found path to node {}: {:?}", graph.target_id(idx).unwrap(), new_path);
                queue.push_back((graph.target_id(idx).unwrap(), new_path));
            }
        }

        debug!("Current queue state: {:?}", queue);
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
        let path = breath_first_search(&graph, 1, 3);
        assert_eq!(path, Some(vec![1, 2, 3]));
    }
}
