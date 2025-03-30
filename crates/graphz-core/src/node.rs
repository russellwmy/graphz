use snafu::prelude::*;
use std::{error::Error, sync::Arc};

use arrow::{
    array::{Array, PrimitiveArray, RecordBatch},
    datatypes::{DataType, Field, Float64Type, Schema, UInt32Type},
};
use derive_builder::Builder;

use crate::types::{NodeId, Position, Weight};

#[derive(Debug, Snafu)]
pub enum NodeDataError {
    IndexOutOfBounds,
    ColumnNotFound,
    ColumnTypeMismatch { data_type: String },
}

type Result<T, E = NodeDataError> = std::result::Result<T, E>;

#[derive(Debug, strum::EnumString, strum::AsRefStr)]
pub enum Attribute {
    // Node
    #[strum(serialize = "node")]
    Node,
    // Weight of the edge: positive flows source->target, negative flows target->source
    #[strum(serialize = "weight")]
    Weight,
    // Position of the node
    // This is a list of floats, representing the x, y, z coordinates of the node
    // in a 3D space
    #[strum(serialize = "position")]
    Position,
}

#[derive(Debug, Clone, PartialEq, Builder)]
pub struct Node {
    pub id: NodeId,
    #[builder(setter(into, strip_option), default)]
    pub position: Option<Position>,
    #[builder(setter(into, strip_option), default)]
    pub weight: Option<Weight>,
}

impl Node {
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeRecordBatch(pub(crate) RecordBatch);

impl NodeRecordBatch {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new(Attribute::Node.as_ref(), DataType::UInt32, false),
            Field::new(Attribute::Weight.as_ref(), DataType::Float64, true),
            Field::new(Attribute::Position.as_ref(), DataType::Float64, true),
        ])
    }

    pub fn record_batch(&self) -> &RecordBatch {
        &self.0
    }

    pub fn num_nodes(&self) -> usize {
        self.record_batch().num_rows()
    }

    pub fn node_id(&self, idx: usize) -> Result<NodeId> {
        if idx >= self.num_nodes() {
            return Err(NodeDataError::IndexOutOfBounds);
        }

        let column = self
            .record_batch()
            .column_by_name(Attribute::Node.as_ref())
            .ok_or(NodeDataError::ColumnNotFound)?;
        let data = column.as_any().downcast_ref::<PrimitiveArray<UInt32Type>>().ok_or(
            NodeDataError::ColumnTypeMismatch { data_type: column.data_type().to_string() },
        )?;

        Ok(data.value(idx))
    }

    pub fn weight(&self, idx: usize) -> Result<Option<Weight>> {
        if idx >= self.num_nodes() {
            return Err(NodeDataError::IndexOutOfBounds);
        }
        let column = self
            .record_batch()
            .column_by_name(Attribute::Weight.as_ref())
            .ok_or(NodeDataError::ColumnNotFound)?;
        let data = column.as_any().downcast_ref::<PrimitiveArray<Float64Type>>().ok_or(
            NodeDataError::ColumnTypeMismatch { data_type: column.data_type().to_string() },
        )?;

        Ok(Some(data.value(idx)))
    }

    pub fn position(&self, idx: usize) -> Result<Option<Position>> {
        if idx >= self.num_nodes() {
            return Err(NodeDataError::IndexOutOfBounds);
        }
        let column = self
            .record_batch()
            .column_by_name(Attribute::Position.as_ref())
            .ok_or(NodeDataError::ColumnNotFound)?;
        let data = column.as_any().downcast_ref::<PrimitiveArray<Float64Type>>().ok_or(
            NodeDataError::ColumnTypeMismatch { data_type: column.data_type().to_string() },
        )?;

        Ok(Some(data.value(idx)))
    }

    pub fn node(&self, idx: usize) -> Result<Node> {
        if idx >= self.num_nodes() {
            return Err(NodeDataError::IndexOutOfBounds);
        }
        let node_id = self.node_id(idx)?;
        let weight = self.weight(idx)?;
        let position = self.position(idx)?;
        Ok(Node { id: node_id, weight, position })
    }

    pub fn nodes(&self) -> Result<Vec<Node>, Box<dyn Error>> {
        let mut nodes = Vec::with_capacity(self.num_nodes());
        for idx in 0..self.num_nodes() {
            let node = self.node(idx)?;
            nodes.push(node);
        }
        Ok(nodes)
    }
}

impl From<RecordBatch> for NodeRecordBatch {
    fn from(record_batch: RecordBatch) -> Self {
        NodeRecordBatch(record_batch)
    }
}

impl From<Vec<Node>> for NodeRecordBatch {
    fn from(nodes: Vec<Node>) -> Self {
        let mut node_ids = Vec::new();
        let mut weights = Vec::new();
        let mut positions = Vec::new();
        for node in nodes {
            node_ids.push(node.id);
            weights.push(node.weight);
            positions.push(node.position);
        }

        let schema = Self::schema();
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(PrimitiveArray::<UInt32Type>::from(node_ids)),
                Arc::new(PrimitiveArray::<Float64Type>::from(weights)),
                Arc::new(PrimitiveArray::<Float64Type>::from(positions)),
            ],
        )
        .unwrap();
        NodeRecordBatch(record_batch)
    }
}
