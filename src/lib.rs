use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

type ByteStr = [u8];
type ByteString = Vec<u8>;

pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}
pub struct KvStore {
    f: File,
    pub index: HashMap<ByteString, u64>,
}

impl KvStore {
    pub fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        let index = HashMap::new();

        Ok(Self { f, index })
    }

    // Build in-memory index of the key-value pairs stored in file
    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);

        loop {
            let position = f.seek(SeekFrom::Current(0))?;
            let maybe_kv = KvStore::process_record(&mut f);

            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };

            self.index.insert(kv.key, position);
        }

        Ok(())
    }

    // Layout:
    // Fixed-width header
    // +------------------------+
    // | Checksum (32 bytes)    |
    // +------------------------+
    // | Key Length (32 bytes)  |
    // +------------------------+
    // | Value Length (32 bytes)|
    // +------------------------+
    //
    // Variable-length body:
    // +------------------------+
    // | Key ([u8; key_len])    |
    // +------------------------+
    // | Value ([u8; value_len])|
    // +------------------------+

    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        // we need to store data in a deterministic way -> diff platform - diff endianness
        // byteorder crate here guarantees how our byte sequences are interpreted

        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let val_len = f.read_u32::<LittleEndian>()?;

        let data_len = key_len + val_len;
        let mut data = ByteString::with_capacity(data_len as usize);

        {
            // f.by_ref() is required because .take(n) creates a new Read instance. Using a reference within this block allows
            // us to sidestep ownership issues, we then read data_len into the data buffer
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }
        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = crc32::checksum_ieee(&data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }

        let val = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair { key, value: val })
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(pos) => *pos,
        };

        let kv = self.get_at(position)?;

        Ok(Some(kv.value))
    }

    pub fn get_at(&mut self, position: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;
        let kv = KvStore::process_record(&mut f)?;

        Ok(kv)
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?; // get position of the start of data

        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);

        let key_len = key.len();
        let val_len = value.len();

        // Buiild check sum
        let mut tmp = ByteString::with_capacity(key_len + val_len);

        for byte in key {
            tmp.push(*byte);
        }

        for byte in value {
            tmp.push(*byte)
        }

        let checksum = crc32::checksum_ieee(&tmp);

        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;

        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(val_len as u32)?;
        f.write_all(&mut tmp)?;

        // We return the position where the data starts as thats what we actually need to
        // store in our index.
        Ok(current_position)
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }
}
