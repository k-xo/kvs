use anyhow::{self};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
};

/// A simple key-value store.
pub struct KvStore {
    // The map that stores key-value positions.
    index: HashMap<String, u64>,
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set(String, String),
    Remove(String),
}

pub type Result<T> = anyhow::Result<T, anyhow::Error>;

impl KvStore {
    /// Opens a `KvStore` with the given file path, creating a new file if it doesn't exist.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file);

        let mut index = HashMap::new();
        let mut pos = 0;

        let mut line = String::new();
        while reader.read_line(&mut line)? != 0 {
            if let Ok(cmd) = serde_json::from_str::<Command>(&line) {
                if let Command::Set(ref key, _) = cmd {
                    index.insert(key.clone(), pos);
                }
            }
            pos = reader.stream_position()?;
            line.clear();
        }

        reader.seek(SeekFrom::Start(0))?;

        Ok(KvStore {
            index,
            writer,
            reader,
        })
    }

    /// Creates a new `KvStore`.
    pub fn new() -> Result<KvStore> {
        let path = "db";
        let writer = BufWriter::new(File::create(path)?);
        let reader = BufReader::new(File::open(path)?);
        Ok(KvStore {
            index: HashMap::new(),
            writer,
            reader,
        })
    }

    /// Sets the value of a key. If the key already exists, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.writer.stream_position()?;
        let cmd = Command::Set(key.clone(), value);
        serde_json::to_writer(&mut self.writer, &cmd)?;
        writeln!(self.writer)?;
        self.writer.flush()?;
        self.index.insert(key, pos);
        Ok(())
    }

    /// Gets the value of a key. Returns `None` if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(&pos) = self.index.get(&key) {
            self.reader.seek(SeekFrom::Start(pos))?;
            let mut cmd_string = String::new();
            self.reader.read_line(&mut cmd_string)?;

            if let Ok(Command::Set(_, value)) = serde_json::from_str(&cmd_string.trim_end()) {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Removes a key from the store. Does nothing if the key does not exist.
    pub fn remove(&mut self, _: String) -> Result<()> {
        unimplemented!();
    }
}
