use crate::{breath_first_search, types::NodeId, Graph};
use std::collections::HashSet;

impl Graph {
    pub fn is_complete(&self) -> bool {
        let num_nodes = self.num_nodes();
        let num_edges = self.num_edges();
        let expected_edges = (num_nodes * (num_nodes - 1)) / 2;

        if expected_edges != num_edges {
            return false;
        }

        if num_nodes == 1 {
            return false;
        }

        return true;
    }

    pub fn is_connected(&self) -> bool {
        if self.num_nodes() == 0 {
            return false;
        }
        let mut node_ids: Vec<NodeId> =
            (0..self.num_nodes()).map(|idx| self.node_id(idx).unwrap()).collect();
        node_ids.sort();

        let result =
            breath_first_search(self, *node_ids.first().unwrap(), *node_ids.last().unwrap());
        println!("Result: {:?}", result);
        result.is_some()
    }

    pub fn is_acyclic(&self) -> bool {
        let mut visited = HashSet::new();
        let mut stack = Vec::new();
        let mut recursion_stack = HashSet::new();

        for i in 0..self.num_nodes() {
            let start_node_id = self.node_id(i).unwrap();
            if !visited.contains(&start_node_id) {
                stack.push(start_node_id);
                recursion_stack.insert(start_node_id);

                while let Some(current_node_id) = stack.pop() {
                    if !visited.insert(current_node_id) {
                        return false;
                    }

                    let neighbors = self.neighbors(current_node_id).unwrap();

                    for neighbor in neighbors {
                        if !visited.contains(&neighbor) {
                            stack.push(neighbor);
                            recursion_stack.insert(neighbor);
                        } else if recursion_stack.contains(&neighbor) {
                            return false;
                        }
                    }
                }
                recursion_stack.clear();
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::{edge::Edge, Graph};

    #[test]
    fn test_is_complete() {
        let edges = vec![
            Edge::builder().source_id(0).target_id(1).build().unwrap(),
            Edge::builder().source_id(0).target_id(2).build().unwrap(),
            Edge::builder().source_id(0).target_id(3).build().unwrap(),
            Edge::builder().source_id(0).target_id(4).build().unwrap(),
            Edge::builder().source_id(1).target_id(2).build().unwrap(),
            Edge::builder().source_id(1).target_id(3).build().unwrap(),
            Edge::builder().source_id(1).target_id(4).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).build().unwrap(),
            Edge::builder().source_id(2).target_id(4).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).build().unwrap(),
        ];

        let graph = Graph::builder().edges(edges).build().unwrap();
        assert_eq!(graph.num_nodes(), 5);
        assert!(graph.is_complete());
    }

    #[test]
    fn test_is_connected() {
        let edges = vec![
            Edge::builder().source_id(0).target_id(1).build().unwrap(),
            Edge::builder().source_id(1).target_id(2).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();

        assert!(graph.is_connected());
    }

    #[test]
    fn test_is_acyclic() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();
        assert!(graph.is_acyclic());
        let edges_with_cycle = vec![
            Edge::builder().source_id(1).target_id(2).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).build().unwrap(),
            Edge::builder().source_id(5).target_id(1).build().unwrap(), // Cycle
        ];
        let graph_with_cycle = Graph::builder().edges(edges_with_cycle).build().unwrap();
        assert!(!graph_with_cycle.is_acyclic());
    }
}
