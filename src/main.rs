//! CLI for Vectrill streaming engine

#[cfg(feature = "cli")]
use clap::Parser;
use vectrill::{operators::{NoOpOperator, Pipeline}, RecordBatch};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[cfg(feature = "cli")]
#[derive(Parser, Debug)]
#[command(name = "vectrill")]
#[command(about = "High-performance Arrow-native streaming engine", long_about = None)]
struct Args {
    /// Process a dummy batch
    #[arg(long)]
    dummy: bool,
}

#[cfg(feature = "cli")]
fn main() {
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    if args.dummy {
        process_dummy_batch();
    } else {
        println!("Vectrill v{}", vectrill::VERSION);
        println!("Use --dummy to process a test batch");
    }
}

#[cfg(not(feature = "cli"))]
fn main() {
    println!("Vectrill v{}", vectrill::VERSION);
    println!("CLI feature not enabled. Build with --features cli to enable CLI.");
}

fn process_dummy_batch() {
    println!("Creating dummy batch...");
    
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let id = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let name = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id), Arc::new(name)])
        .expect("Failed to create batch");

    println!("Created batch with {} rows", batch.num_rows());

    println!("Processing through pipeline...");
    let mut pipeline = Pipeline::new();
    pipeline.add_operator(Box::new(NoOpOperator));

    let result = pipeline.execute(batch).expect("Pipeline execution failed");
    println!("Pipeline output: {} rows", result.num_rows());

    println!("Flushing pipeline...");
    let flushed = pipeline.flush().expect("Flush failed");
    println!("Flushed {} batches", flushed.len());

    println!("Done!");
}
