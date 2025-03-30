use arrow::array::RecordBatch;
use derive_builder::Builder;
use snafu::prelude::*;
use std::sync::Arc;

use crate::{
    edge::{Edge, EdgeDataError, EdgeRecordBatch},
    node::{Node, NodeDataError, NodeRecordBatch},
    types::NodeId,
};

#[derive(Debug, Snafu)]
pub enum GraphError {
    InvalidNodeSchema,
    InvalidEdgeSchema,
    EmptyGraph { name: String },
}

#[derive(Debug)]
pub struct Graph {
    node_record_batch: Arc<NodeRecordBatch>,
    edge_record_batch: Arc<EdgeRecordBatch>,
}

impl Graph {
    pub fn builder() -> GraphBuilder {
        GraphBuilder::default()
    }

    pub fn from_arrow_record_batches(
        node_record_batch: NodeRecordBatch,
        edge_record_batch: EdgeRecordBatch,
    ) -> Result<Self, GraphError> {
        if NodeRecordBatch::schema() != *node_record_batch.record_batch().schema() {
            return Err(GraphError::InvalidNodeSchema);
        }
        if EdgeRecordBatch::schema() != *edge_record_batch.record_batch().schema() {
            return Err(GraphError::InvalidEdgeSchema);
        }
        Ok(Self {
            node_record_batch: Arc::new(node_record_batch),
            edge_record_batch: Arc::new(edge_record_batch),
        })
    }

    pub fn node_record_batch(&self) -> Arc<RecordBatch> {
        self.node_record_batch.record_batch().clone().into()
    }

    pub fn edge_record_batch(&self) -> Arc<RecordBatch> {
        self.edge_record_batch.record_batch().clone().into()
    }

    pub fn num_nodes(&self) -> usize {
        self.node_record_batch.num_nodes()
    }

    pub fn node_id(&self, idx: usize) -> Result<NodeId, NodeDataError> {
        self.node_record_batch.node_id(idx)
    }

    pub fn num_edges(&self) -> usize {
        self.edge_record_batch.num_edges()
    }

    pub fn source_id(&self, idx: usize) -> Result<NodeId, EdgeDataError> {
        self.edge_record_batch.source_id(idx)
    }

    pub fn target_id(&self, idx: usize) -> Result<NodeId, EdgeDataError> {
        self.edge_record_batch.target_id(idx)
    }

    pub fn weight(&self, idx: usize) -> Result<Option<f64>, EdgeDataError> {
        self.edge_record_batch.weight(idx)
    }

    pub fn neighbors(&self, node: NodeId) -> Result<Vec<NodeId>, EdgeDataError> {
        self.edge_record_batch.neighbors(node)
    }

    pub fn neighbors_with_weights(
        &self,
        node: NodeId,
    ) -> Result<Vec<(NodeId, f64)>, EdgeDataError> {
        self.edge_record_batch.neighbors_with_weights(node)
    }
}

#[derive(Debug, Builder)]
#[builder(build_fn(skip), name = "GraphBuilder")]
pub struct GraphData {
    #[allow(unused)]
    nodes: Vec<Node>,
    #[allow(unused)]
    edges: Vec<Edge>,
}

impl GraphBuilder {
    pub fn build(&self) -> Result<Graph, GraphError> {
        let edges =
            self.edges.as_ref().ok_or(GraphError::EmptyGraph { name: "no edges".to_string() })?;

        let nodes = match self.nodes.as_ref() {
            Some(nodes) => nodes.clone(),
            None => {
                let mut nodes = Vec::new();
                for edge in edges.clone() {
                    if !nodes.iter().any(|node: &Node| node.id == edge.source_id) {
                        nodes.push(Node { id: edge.source_id, weight: None, position: None });
                    }
                    if !nodes.iter().any(|node: &Node| node.id == edge.target_id) {
                        nodes.push(Node { id: edge.target_id, weight: None, position: None });
                    }
                }
                nodes
            }
        };
        let edge_record_batch = EdgeRecordBatch::from(edges.clone());
        let node_record_batch = NodeRecordBatch::from(nodes);

        let graph = Graph::from_arrow_record_batches(node_record_batch, edge_record_batch)?;
        Ok(graph)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_with_weight() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).weight(1.0).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).weight(2.0).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).weight(3.0).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).weight(4.0).build().unwrap(),
            Edge::builder().source_id(5).target_id(6).weight(5.0).build().unwrap(),
        ];
        let graph = Graph::builder().edges(edges).build().unwrap();
        assert_eq!(graph.num_nodes(), 6);

        let num_edges = graph.num_edges();
        assert_eq!(num_edges, 5);
        for idx in 0..num_edges {
            assert_eq!(graph.source_id(idx).unwrap(), (idx + 1) as NodeId);
        }
        for idx in 0..num_edges {
            assert_eq!(graph.target_id(idx).unwrap(), (idx + 2) as NodeId);
        }
        for idx in 0..num_edges {
            assert_eq!(graph.weight(idx).unwrap(), Some((idx + 1) as f64));
        }
    }

    #[test]
    fn test_graph() {
        let edges = vec![
            Edge::builder().source_id(1).target_id(2).build().unwrap(),
            Edge::builder().source_id(2).target_id(3).build().unwrap(),
            Edge::builder().source_id(3).target_id(4).build().unwrap(),
            Edge::builder().source_id(4).target_id(5).build().unwrap(),
            Edge::builder().source_id(5).target_id(6).build().unwrap(),
        ];

        let graph = Graph::builder().edges(edges).build().unwrap();
        assert_eq!(graph.num_nodes(), 6);

        let num_edges = graph.num_edges();
        assert_eq!(num_edges, 5);
        for idx in 0..num_edges {
            assert_eq!(graph.source_id(idx).unwrap(), (idx + 1) as NodeId);
        }
        for idx in 0..num_edges {
            assert_eq!(graph.target_id(idx).unwrap(), (idx + 2) as NodeId);
        }
    }
}
