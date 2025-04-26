use std::{
    cmp::Ordering,
    io::{Error, ErrorKind, Result},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Clone, Debug)]
pub struct Record {
    pub key: String,
    pub value: MemValue,
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Record {}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl Record {
    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<u64> {
        let key_bytes = self.key.as_bytes();
        let val_bytes = self.value.serialize();
        let tag = self.value.type_tag();

        let key_len = key_bytes.len() as u16;
        let val_len = val_bytes.len() as u16;

        let mut offset = 0;
        offset += writer.write(&key_len.to_be_bytes()).await? as u64;
        offset += writer.write(&val_len.to_be_bytes()).await? as u64;
        offset += writer.write(&[tag]).await? as u64;
        offset += writer.write(key_bytes).await? as u64;
        offset += writer.write(&val_bytes).await? as u64;

        Ok(offset)
    }

    pub async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let mut len_buf = [0u8; 2];

        // Read key length
        reader.read_exact(&mut len_buf).await?;
        let key_len = u16::from_be_bytes(len_buf) as usize;

        // Read value length
        reader.read_exact(&mut len_buf).await?;
        let val_len = u16::from_be_bytes(len_buf) as usize;

        // Read value tag
        let mut tag_buf = [0u8; 1];
        reader.read_exact(&mut tag_buf).await?;
        let tag = tag_buf[0];

        // Read key
        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf).await?;
        let key = String::from_utf8(key_buf)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid UTF-8 in key"))?;

        // Read value
        let mut val_buf = vec![0u8; val_len];
        reader.read_exact(&mut val_buf).await?;

        // Deserialize value from tag + bytes
        let value = MemValue::deserialize(tag, &val_buf)?;

        Ok(Record { key, value })
    }
}

#[derive(Clone, Debug)]
pub enum MemValue {
    Value(Value),
    Tombstone,
}

#[derive(Clone, Debug)]
pub enum Value {
    Str(String),
    Int64(i64),
    Float64(f64),
}

impl MemValue {
    pub fn to_value(self) -> Option<Value> {
        match self {
            MemValue::Tombstone => None,
            MemValue::Value(value) => Some(value),
        }
    }

    pub fn type_tag(&self) -> u8 {
        match self {
            MemValue::Value(Value::Str(_)) => 0,
            MemValue::Value(Value::Int64(_)) => 1,
            MemValue::Value(Value::Float64(_)) => 2,
            MemValue::Tombstone => 255,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            MemValue::Value(Value::Str(s)) => s.as_bytes().to_vec(),
            MemValue::Value(Value::Int64(i)) => i.to_be_bytes().to_vec(),
            MemValue::Value(Value::Float64(f)) => f.to_be_bytes().to_vec(),
            MemValue::Tombstone => vec![],
        }
    }

    pub fn deserialize(tag: u8, bytes: &[u8]) -> Result<Self> {
        match tag {
            0 => {
                let parsed = String::from_utf8(bytes.to_vec()).map_err(|_| {
                    Error::new(ErrorKind::InvalidData, "Unable to deserialize record")
                })?;
                Ok(MemValue::Value(Value::Str(parsed)))
            }
            1 if bytes.len() == 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(bytes);
                Ok(MemValue::Value(Value::Int64(i64::from_be_bytes(buf))))
            }
            2 if bytes.len() == 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(bytes);
                Ok(MemValue::Value(Value::Float64(f64::from_be_bytes(buf))))
            }
            255 => Ok(MemValue::Tombstone),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Unable to deserialize record",
            )),
        }
    }
}
