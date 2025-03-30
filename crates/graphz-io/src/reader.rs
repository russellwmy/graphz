use arrow::{error::ArrowError, ipc::reader::FileReader};
use graphz_core::{edge::EdgeRecordBatch, node::NodeRecordBatch, Graph};
use snafu::prelude::*;
use std::{fs::File, path::PathBuf};

#[derive(Debug, Snafu)]
pub enum ReadGraphError {
    FileNotFound,
    ArrowError { source: ArrowError },
    InvalidGraphFormat,
}
type Result<T, E = ReadGraphError> = std::result::Result<T, E>;

pub fn read_graph_from_arrow_files(path: &str) -> Result<Graph> {
    let target = PathBuf::from(path);
    let mut files = target.read_dir().map_err(|_| ReadGraphError::FileNotFound)?;
    let nodes_path = files.find_map(|file| {
        if file.is_ok()
            && file.as_ref().unwrap().path().is_file()
            && file.as_ref().unwrap().path().to_str().unwrap().contains("graph.nodes.arrow")
        {
            Some(file.unwrap().path())
        } else {
            None
        }
    });
    let edges_path = files.find_map(|file| {
        if file.is_ok()
            && file.as_ref().unwrap().path().is_file()
            && file.as_ref().unwrap().path().to_str().unwrap().contains("graph.edges.arrow")
        {
            Some(file.unwrap().path())
        } else {
            None
        }
    });
    if nodes_path.is_none() || edges_path.is_none() {
        return Err(ReadGraphError::FileNotFound);
    }
    let nodes_path = nodes_path.unwrap();
    let edges_path = edges_path.unwrap();

    let mut node_record_batch = FileReader::try_new(
        File::open(nodes_path)
            .map_err(|source| ReadGraphError::ArrowError { source: source.into() })?,
        None,
    )
    .map_err(|source| ReadGraphError::ArrowError { source })?;
    let node_record_batch = node_record_batch.next().unwrap().unwrap();
    let mut edges_record_batch = FileReader::try_new(
        File::open(edges_path)
            .map_err(|source| ReadGraphError::ArrowError { source: source.into() })?,
        None,
    )
    .map_err(|source| ReadGraphError::ArrowError { source })?;
    let edges_record_batch = edges_record_batch.next().unwrap().unwrap();

    let graph = Graph::from_arrow_record_batches(
        NodeRecordBatch::from(node_record_batch),
        EdgeRecordBatch::from(edges_record_batch),
    )
    .map_err(|_| ReadGraphError::InvalidGraphFormat)?;

    Ok(graph)
}
