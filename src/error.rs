use thiserror::Error;
use std::io;
use postcard;
use atomicwrites;

#[derive(Error, Debug)]
pub enum MmapVecDequeError {
  #[error("I/O error: {0}")]
  Io(#[from] io::Error),

  #[error("Serialization/Deserialization error: {0}")]
  Serde(#[from] postcard::Error),

  // Removed the Mmap variant since memmap2 doesn't provide a MmapError type.
  // All mmap errors are covered by io::Error anyway.

  #[error("Atomic write error: {0}")]
  AtomicWrite(#[from] atomicwrites::Error<std::io::Error>),

  #[error("Type mismatch: stored type `{stored}`, requested type `{requested}`")]
  TypeMismatch { stored: String, requested: String },

  #[error("Element size mismatch: stored size `{stored}`, requested size `{requested}`")]
  ElementSizeMismatch { stored: usize, requested: usize },

  #[error("Zero-sized types are not supported")]
  ZeroSizedType,

  #[error("Index out of range")]
  IndexOutOfRange,

  #[error("Chunk size mismatch: stored size `{stored}`, requested size `{requested}`")]
  ChunkSizeMismatch { stored: usize, requested: usize },

  #[error("Other error: {0}")]
  Other(String),
}
