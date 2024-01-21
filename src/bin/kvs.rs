#![allow(unused_variables)]

use clap::{Parser, Subcommand};
use kvs::{KvStore, Result};
use std::process;

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
    /// Get a value from the store
    Get { key: String },
    /// Remove a value from the store
    Rm { key: String },
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut store = KvStore::open("db").expect("unable to create KvStore");

    match args.command {
        Commands::Set { key, value } => {
            store.set(key, value)?;
            process::exit(0);
        }
        Commands::Get { key } => {
            match store.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
            process::exit(0);
        }
        Commands::Rm { key } => {
            eprintln!("unimplemented");
            process::exit(1);
        }
    }
}
