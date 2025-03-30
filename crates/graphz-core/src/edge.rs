use crate::types::{NodeId, Weight};
use arrow::{
    array::{PrimitiveArray, RecordBatch},
    datatypes::{DataType, Field, Float64Type, Schema, UInt32Type},
    error::ArrowError,
};
use derive_builder::Builder;
use snafu::prelude::*;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum EdgeDataError {
    IndexOutOfBounds,
    ColumnNotFound,
    ColumnTypeMismatch { data_type: String },
    FailedToAddEdges { source: ArrowError },
}

type Result<T, E = EdgeDataError> = std::result::Result<T, E>;

#[derive(Debug, strum::EnumString, strum::AsRefStr)]
pub enum Attribute {
    // Source
    #[strum(serialize = "source")]
    Source,
    // Target
    #[strum(serialize = "target")]
    Target,

    // Weight of the edge: positive flows source->target, negative flows target->source
    #[strum(serialize = "weight")]
    Weight,
}

#[derive(Debug, Clone, Copy, PartialEq, Builder)]
pub struct Edge {
    pub source_id: NodeId,
    pub target_id: NodeId,
    #[builder(setter(into, strip_option), default)]
    pub weight: Option<Weight>,
}

impl Edge {
    pub fn builder() -> EdgeBuilder {
        EdgeBuilder::default()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EdgeRecordBatch(RecordBatch);

impl EdgeRecordBatch {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new(Attribute::Source.as_ref(), DataType::UInt32, false),
            Field::new(Attribute::Target.as_ref(), DataType::UInt32, false),
            Field::new(Attribute::Weight.as_ref(), DataType::Float64, true),
        ])
    }

    pub fn record_batch(&self) -> &RecordBatch {
        &self.0
    }

    pub fn num_edges(&self) -> usize {
        self.record_batch().num_rows()
    }

    pub fn source_id(&self, idx: usize) -> Result<NodeId> {
        if idx >= self.num_edges() {
            return Err(EdgeDataError::IndexOutOfBounds);
        }
        let column = self
            .record_batch()
            .column_by_name(Attribute::Source.as_ref())
            .ok_or(EdgeDataError::ColumnNotFound)?;
        let data = column.as_any().downcast_ref::<PrimitiveArray<UInt32Type>>().ok_or(
            EdgeDataError::ColumnTypeMismatch { data_type: column.data_type().to_string() },
        )?;
        Ok(data.value(idx))
    }

    pub fn target_id(&self, idx: usize) -> Result<NodeId> {
        if idx >= self.num_edges() {
            return Err(EdgeDataError::IndexOutOfBounds);
        }
        let column = self
            .record_batch()
            .column_by_name(Attribute::Target.as_ref())
            .ok_or(EdgeDataError::ColumnNotFound)?;
        let data = column.as_any().downcast_ref::<PrimitiveArray<UInt32Type>>().ok_or(
            EdgeDataError::ColumnTypeMismatch { data_type: column.data_type().to_string() },
        )?;
        Ok(data.value(idx))
    }
    pub fn neighbors(&self, node: NodeId) -> Result<Vec<NodeId>> {
        let mut neighbors = Vec::new();
        for i in 0..self.num_edges() {
            let edge = self.edge(i)?;
            if edge.source_id == node {
                neighbors.push(edge.target_id);
            }
        }
        Ok(neighbors)
    }

    pub fn weight(&self, idx: usize) -> Result<Option<Weight>> {
        if idx >= self.num_edges() {
            return Err(EdgeDataError::IndexOutOfBounds);
        }
        let column = self
            .record_batch()
            .column_by_name(Attribute::Weight.as_ref())
            .ok_or(EdgeDataError::ColumnNotFound)?;
        let data = column.as_any().downcast_ref::<PrimitiveArray<Float64Type>>().ok_or(
            EdgeDataError::ColumnTypeMismatch { data_type: column.data_type().to_string() },
        )?;
        Ok(data.value(idx).into())
    }

    pub fn neighbors_with_weights(&self, node: NodeId) -> Result<Vec<(NodeId, Weight)>> {
        let mut neighbors = Vec::new();
        for i in 0..self.num_edges() {
            let edge = self.edge(i)?;
            if edge.source_id == node {
                neighbors.push((edge.target_id, edge.weight.unwrap_or_default()));
            }
        }
        Ok(neighbors)
    }

    pub fn edge(&self, idx: usize) -> Result<Edge> {
        if idx >= self.num_edges() {
            return Err(EdgeDataError::IndexOutOfBounds);
        }
        let source_id = self.source_id(idx)?;
        let target_id = self.target_id(idx)?;
        let weight = self.weight(idx)?;
        Ok(Edge { source_id, target_id, weight })
    }
    pub fn edges(&self) -> Result<Vec<Edge>> {
        let mut edges = Vec::with_capacity(self.num_edges());
        for idx in 0..self.num_edges() {
            let edge = self.edge(idx)?;
            edges.push(edge);
        }
        Ok(edges)
    }

    pub fn add_edges(&mut self, edges: Vec<Edge>) -> Result<()> {
        let record_batch = self.record_batch().clone();
        let source_column = record_batch
            .column_by_name(Attribute::Source.as_ref())
            .ok_or(EdgeDataError::ColumnNotFound)?;
        let target_column = record_batch
            .column_by_name(Attribute::Target.as_ref())
            .ok_or(EdgeDataError::ColumnNotFound)?;
        let weight_column = record_batch
            .column_by_name(Attribute::Weight.as_ref())
            .ok_or(EdgeDataError::ColumnNotFound)?;
        let source_data = source_column
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt32Type>>()
            .ok_or(EdgeDataError::ColumnTypeMismatch {
                data_type: source_column.data_type().to_string(),
            })?;
        let target_data = target_column
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt32Type>>()
            .ok_or(EdgeDataError::ColumnTypeMismatch {
                data_type: target_column.data_type().to_string(),
            })?;
        let weight_data = weight_column
            .as_any()
            .downcast_ref::<PrimitiveArray<Float64Type>>()
            .ok_or(EdgeDataError::ColumnTypeMismatch {
                data_type: weight_column.data_type().to_string(),
            })?;
        let new_source = source_data
            .values()
            .to_vec()
            .into_iter()
            .chain(edges.iter().map(|edge| edge.source_id))
            .collect::<Vec<_>>();
        let new_target = target_data
            .values()
            .to_vec()
            .into_iter()
            .chain(edges.iter().map(|edge| edge.target_id))
            .collect::<Vec<_>>();
        let new_weight = weight_data
            .values()
            .to_vec()
            .into_iter()
            .chain(edges.iter().map(|edge| edge.weight.unwrap_or_default()))
            .collect::<Vec<_>>();
        let new_edges = RecordBatch::try_new(
            self.record_batch().schema(),
            vec![
                Arc::new(arrow::array::UInt32Array::from(new_source)),
                Arc::new(arrow::array::UInt32Array::from(new_target)),
                Arc::new(arrow::array::Float64Array::from(new_weight)),
            ],
        )
        .context(FailedToAddEdgesSnafu {})?;

        self.0 = new_edges;
        Ok(())
    }
}

impl From<RecordBatch> for EdgeRecordBatch {
    fn from(record_batch: RecordBatch) -> Self {
        Self(record_batch)
    }
}

impl From<Vec<Edge>> for EdgeRecordBatch {
    fn from(edges: Vec<Edge>) -> Self {
        let mut source_id = Vec::new();
        let mut target_id = Vec::new();
        let mut weight = Vec::new();

        for edge in edges {
            source_id.push(edge.source_id);
            target_id.push(edge.target_id);
            weight.push(edge.weight.unwrap_or_default());
        }

        let record_batch = RecordBatch::try_new(
            Self::schema().into(),
            vec![
                Arc::new(PrimitiveArray::<UInt32Type>::from(source_id)),
                Arc::new(PrimitiveArray::<UInt32Type>::from(target_id)),
                Arc::new(PrimitiveArray::<Float64Type>::from(weight)),
            ],
        )
        .unwrap();

        Self(record_batch)
    }
}
