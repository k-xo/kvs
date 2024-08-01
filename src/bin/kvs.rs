#![allow(unused)]
use std::path::Path;

use clap::{Parser, Subcommand};
use kvs::KvStore;

/// Simple Key Value Store
#[derive(Parser, Debug)]
#[command(name="kvs", author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Set a value in the store
    Set { key: String, value: String },
    /// Updates a value in the store
    Update { key: String, value: String },
    /// Get a value from the store
    Get { key: String },
    /// Remove a value from the store
    Del { key: String },
}

fn main() {
    let args = Args::parse();
    let mut store = KvStore::open(Path::new("db")).unwrap();
    store.load().unwrap();

    match args.command {
        Commands::Get { key } => match store.get(key.as_ref()) {
            Ok(Some(v)) => println!("{}", String::from_utf8_lossy(&v)),
            Ok(None) => println!("not found"),
            Err(e) => println!("{}", e),
        },

        Commands::Set { key, value } => {
            println!("{:?}", store.insert(key.as_ref(), value.as_ref()))
        }

        Commands::Update { key, value } => match store.update(key.as_ref(), value.as_ref()) {
            Ok(()) => println!("Updated successfully"),
            Err(e) => println!("Update failed: {}", e),
        },

        Commands::Del { key } => match store.delete(key.as_ref()) {
            Ok(()) => println!("Deleted successfully"),
            Err(e) => println!("Deletion failed: {}", e),
        },
    }
}
