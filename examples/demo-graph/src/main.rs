use graphz_core::{breath_first_search, edge::Edge, Graph};
use snafu::Whatever;

fn build_big_graph() -> Result<(), Whatever> {
    tracing_subscriber::fmt()
        .with_level(true)
        .with_writer(std::io::stdout) // Output to standard output
        .init();

    let num_nodes = 1000;
    let num_edges = num_nodes * 5;

    let mut edges = Vec::with_capacity(num_edges);
    // Add edges
    for _ in 0..num_edges {
        let src = rand::random_range(0..num_nodes);
        let dst = rand::random_range(0..num_nodes);
        edges.push(Edge::builder().source_id(src as u32).target_id(dst as u32).build().unwrap());
    }
    println!("Edges created: {}", edges.len());

    // Build the graph
    let g = Graph::builder().edges(edges).build().unwrap();
    assert_eq!(g.num_nodes(), num_nodes);
    assert_eq!(g.num_edges(), num_edges);

    let start = rand::random_range(0..num_nodes) as u32;
    let end = rand::random_range(0..num_nodes) as u32;

    println!("Finding path from {} to {}", start, end);
    let path = breath_first_search(&g, start, end);

    if path.is_none() {
        println!("No path found from {} to {}", start, end);
    } else {
        println!("Path from {} to {}: {:?}", start, end, path);
    }
    Ok(())
}

fn main() {
    // Build a big graph
    if let Err(e) = build_big_graph() {
        eprintln!("Error building graph: {}", e);
        return;
    }
    println!("Graph built successfully!");
}
