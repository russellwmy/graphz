use arrow::ipc::writer::FileWriter;
use graphz_core::{edge::EdgeRecordBatch, node::NodeRecordBatch, Graph};
use snafu::prelude::*;
use std::{
    fs::{create_dir_all, File},
    io::Error,
    path::PathBuf,
};

#[derive(Debug, Snafu)]
pub enum WriteGraphError {
    CreateDirError { source: Error },
}
type Result<T, E = WriteGraphError> = std::result::Result<T, E>;

pub fn write_graph_to_arrow_files(graph: &Graph, path: &str) -> Result<()> {
    let output_path = PathBuf::from(path);
    if !output_path.exists() {
        create_dir_all(&output_path).context(CreateDirSnafu {})?;
    }

    let mut nodes_path = PathBuf::from(path);
    nodes_path.push(format!("graph.nodes.arrow"));

    let mut edges_path = PathBuf::from(path);
    edges_path.push(format!("graph.edges.arrow"));

    let schema = NodeRecordBatch::schema();
    let mut writer = FileWriter::try_new(File::create(nodes_path).unwrap(), &schema).unwrap();
    writer.write(&graph.node_record_batch()).unwrap();
    writer.finish().unwrap();

    let schema = EdgeRecordBatch::schema();
    let mut writer = FileWriter::try_new(File::create(edges_path).unwrap(), &schema).unwrap();
    writer.write(&graph.edge_record_batch()).unwrap();
    writer.finish().unwrap();

    Ok(())
}
