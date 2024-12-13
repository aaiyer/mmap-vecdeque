use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use serde::{Serialize, Deserialize};
use std::fs::{self, OpenOptions, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{ptr};
use memmap2::{MmapMut, MmapOptions};
use std::marker::PhantomData;
use atomicwrites::{AtomicFile, AllowOverwrite};

const DEFAULT_CHUNK_SIZE: usize = 10_000;
const LARGE_OFFSET: u64 = 1 << 32;

#[derive(Serialize, Deserialize, Debug)]
struct Metadata {
  type_name: String,
  element_size: usize,
  chunk_size: usize,
  start: u64,
  end: u64,
}

impl Metadata {
  fn len(&self) -> usize {
    (self.end - self.start) as usize
  }
}

struct Chunk {
  mmap: MmapMut,
  file: File,
}

pub struct MmapVecDeque<T: Copy> {
  dir: PathBuf,
  meta: Mutex<Metadata>,
  chunks: Mutex<Vec<Chunk>>,
  base_chunk: Mutex<u64>, // Tracks which chunk index corresponds to chunks[0]
  _marker: PhantomData<T>,
  dirty: Mutex<bool>,
}

impl<T: Copy> MmapVecDeque<T> {
  pub fn open_or_create(dir: &Path, chunk_size: Option<usize>) -> Result<Self> {
    let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
    let element_size = size_of::<T>();
    if element_size == 0 {
      bail!("Zero-sized types are not supported");
    }

    if !dir.exists() {
      fs::create_dir_all(dir).context("creating directory for MmapVecDeque")?;
    }

    let metadata_file = dir.join("metadata.bin");
    let type_name = std::any::type_name::<T>().to_string();

    let meta = if metadata_file.exists() {
      let data = fs::read(&metadata_file)?;
      let meta: Metadata = postcard::from_bytes(&data)?;
      if meta.element_size != element_size {
        bail!("Stored element size ({}) does not match requested element size ({})", meta.element_size, element_size);
      }
      if meta.type_name != type_name {
        bail!("Stored type ({}) does not match requested type ({})", meta.type_name, type_name);
      }
      if meta.chunk_size != chunk_size {
        bail!("Stored chunk size ({}) does not match requested chunk size ({})", meta.chunk_size, chunk_size);
      }
      meta
    } else {
      let meta = Metadata {
        type_name: type_name.clone(),
        element_size,
        chunk_size,
        start: LARGE_OFFSET,
        end: LARGE_OFFSET,
      };
      Self::atomic_write_metadata(dir, &meta)?;
      meta
    };

    let deque = MmapVecDeque {
      dir: dir.to_path_buf(),
      meta: Mutex::new(meta),
      chunks: Mutex::new(Vec::new()),
      base_chunk: Mutex::new(0),
      _marker: PhantomData,
      dirty: Mutex::new(false),
    };

    deque.load_chunks()?;
    Ok(deque)
  }

  fn atomic_write_metadata(dir: &Path, meta: &Metadata) -> Result<()> {
    let data = postcard::to_stdvec(meta)?;
    let af = AtomicFile::new(dir.join("metadata.bin"), AllowOverwrite);
    af.write(|f| {
      f.write_all(&data)
    })?;
    let dir_file = OpenOptions::new().read(true).open(dir)?;
    dir_file.sync_all()?;
    Ok(())
  }

  fn load_chunks(&self) -> Result<()> {
    let meta = self.meta.lock();
    let start_chunk = meta.start / meta.chunk_size as u64;
    let end_chunk = if meta.start == meta.end {
      start_chunk
    } else {
      (meta.end - 1) / meta.chunk_size as u64
    };
    let chunk_count = if start_chunk > end_chunk {
      1
    } else {
      (end_chunk - start_chunk) + 1
    };
    drop(meta);

    let mut chunks = self.chunks.lock();
    chunks.clear();
    for ch in start_chunk..(start_chunk + chunk_count) {
      let (mmap, file) = self.open_chunk(ch, true)?;
      chunks.push(Chunk { mmap, file });
    }
    drop(chunks);

    *self.base_chunk.lock() = start_chunk;
    Ok(())
  }

  fn chunk_path(&self, index: u64) -> PathBuf {
    self.dir.join(format!("chunk_{}.bin", index))
  }

  fn open_chunk(&self, index: u64, create: bool) -> Result<(MmapMut, File)> {
    let meta = self.meta.lock();
    let chunk_byte_size = meta.chunk_size * meta.element_size;
    drop(meta);

    let path = self.chunk_path(index);
    if create && !path.exists() {
      let f = OpenOptions::new().write(true).create(true).open(&path)?;
      f.set_len(chunk_byte_size as u64)?;
      f.sync_all()?;
    }
    let file = OpenOptions::new().read(true).write(true).open(&path)?;
    let mmap = unsafe {
      MmapOptions::new()
        .len(chunk_byte_size)
        .map_mut(&file)?
    };
    Ok((mmap, file))
  }

  fn flush_all_chunks(&self) -> Result<()> {
    let chunks = self.chunks.lock();
    for chunk in chunks.iter() {
      chunk.mmap.flush()?;
      chunk.file.sync_all()?;
    }
    Ok(())
  }

  fn global_to_local(&self, index: u64) -> (usize, usize) {
    let meta = self.meta.lock();
    let chunk_size = meta.chunk_size as u64;
    drop(meta);

    let base = *self.base_chunk.lock();
    let chunk_idx = ((index / chunk_size) - base) as usize;
    let elem_idx = (index % chunk_size) as usize;
    (chunk_idx, elem_idx)
  }

  fn ensure_capacity_for(&self, index: u64) -> Result<()> {
    let meta = self.meta.lock();
    let chunk_size = meta.chunk_size as u64;
    let needed_chunk = index / chunk_size;
    drop(meta);

    let mut chunks = self.chunks.lock();
    let base = *self.base_chunk.lock();
    let current_count = chunks.len() as u64;
    if current_count == 0 {
      let (mmap, file) = self.open_chunk(needed_chunk, true)?;
      chunks.push(Chunk { mmap, file });
      drop(chunks);
      *self.base_chunk.lock() = needed_chunk;
      return Ok(());
    }

    let current_start_chunk = base;
    let current_end_chunk = current_start_chunk + current_count - 1;

    if needed_chunk > current_end_chunk {
      // add chunks at the end
      for new_idx in (current_end_chunk+1)..=needed_chunk {
        let (mmap, file) = self.open_chunk(new_idx, true)?;
        chunks.push(Chunk { mmap, file });
      }
    } else if needed_chunk < current_start_chunk {
      // add chunks at the front
      for new_idx in (needed_chunk..current_start_chunk).rev() {
        let (mmap, file) = self.open_chunk(new_idx, true)?;
        chunks.insert(0, Chunk { mmap, file });
      }
      drop(chunks);
      *self.base_chunk.lock() = needed_chunk;
      return Ok(());
    }
    drop(chunks);
    Ok(())
  }

  fn write_element(&self, index: u64, value: T) -> Result<()> {
    self.ensure_capacity_for(index)?;
    let (chunk_idx, elem_idx) = self.global_to_local(index);
    let chunks = self.chunks.lock();
    let meta = self.meta.lock();
    let element_size = meta.element_size;
    drop(meta);

    if chunk_idx >= chunks.len() {
      bail!("Index out of range after ensuring capacity");
    }

    let mmap = &chunks[chunk_idx].mmap;
    let ptr = mmap.as_ptr() as *mut u8;
    unsafe {
      let elem_ptr = ptr.add(elem_idx * element_size) as *mut T;
      ptr::write(elem_ptr, value);
    }
    *self.dirty.lock() = true;
    Ok(())
  }

  fn read_element(&self, index: u64) -> Result<T> {
    let (chunk_idx, elem_idx) = self.global_to_local(index);
    let chunks = self.chunks.lock();
    let meta = self.meta.lock();
    let element_size = meta.element_size;
    drop(meta);

    if chunk_idx >= chunks.len() {
      bail!("Index out of range");
    }
    let mmap = &chunks[chunk_idx].mmap;
    let ptr = mmap.as_ptr();
    unsafe {
      let elem_ptr = ptr.add(elem_idx * element_size) as *const T;
      Ok(ptr::read(elem_ptr))
    }
  }

  pub fn len(&self) -> usize {
    let meta = self.meta.lock();
    meta.len()
  }

  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  pub fn push_back(&mut self, value: T) -> Result<()> {
    let mut meta = self.meta.lock();
    let pos = meta.end;
    meta.end += 1;
    drop(meta);

    self.write_element(pos, value)?;
    Ok(())
  }

  pub fn push_front(&mut self, value: T) -> Result<()> {
    let mut meta = self.meta.lock();
    meta.start = meta.start - 1;
    let pos = meta.start;
    drop(meta);

    self.write_element(pos, value)?;
    Ok(())
  }

  pub fn pop_back(&mut self) -> Result<Option<T>> {
    let mut meta = self.meta.lock();
    if meta.start == meta.end {
      return Ok(None);
    }
    let pos = meta.end - 1;
    meta.end = pos;
    drop(meta);

    let val = self.read_element(pos)?;
    Ok(Some(val))
  }

  pub fn pop_front(&mut self) -> Result<Option<T>> {
    let mut meta = self.meta.lock();
    if meta.start == meta.end {
      return Ok(None);
    }
    let pos = meta.start;
    meta.start = pos + 1;
    drop(meta);

    let val = self.read_element(pos)?;
    Ok(Some(val))
  }

  pub fn front(&self) -> Option<&T> {
    if self.is_empty() {
      return None;
    }
    self.get(0)
  }

  pub fn back(&self) -> Option<&T> {
    let l = self.len();
    if l == 0 {
      return None;
    }
    self.get(l - 1)
  }

  pub fn clear(&mut self) -> Result<()> {
    let mut meta = self.meta.lock();
    meta.start = LARGE_OFFSET;
    meta.end = LARGE_OFFSET;
    drop(meta);
    Ok(())
  }

  pub fn get(&self, index: usize) -> Option<&T> {
    let meta = self.meta.lock();
    if index >= meta.len() {
      return None;
    }
    let global_idx = meta.start + index as u64;
    let element_size = meta.element_size;
    drop(meta);

    let (chunk_idx, elem_idx) = self.global_to_local(global_idx);
    let chunks = self.chunks.lock();
    if chunk_idx >= chunks.len() {
      return None;
    }
    let mmap = &chunks[chunk_idx].mmap;
    let ptr = mmap.as_ptr();
    unsafe {
      let elem_ptr = ptr.add(elem_idx * element_size) as *const T;
      Some(&*elem_ptr)
    }
  }

  pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
    let meta = self.meta.lock();
    if index >= meta.len() {
      return None;
    }
    let global_idx = meta.start + index as u64;
    let element_size = meta.element_size;
    drop(meta);

    let (chunk_idx, elem_idx) = self.global_to_local(global_idx);
    let mut chunks = self.chunks.lock();
    if chunk_idx >= chunks.len() {
      return None;
    }
    let mmap = &mut chunks[chunk_idx].mmap;
    let ptr = mmap.as_mut_ptr();
    unsafe {
      let elem_ptr = ptr.add(elem_idx * element_size) as *mut T;
      *self.dirty.lock() = true;
      Some(&mut *elem_ptr)
    }
  }

  pub fn commit(&self) -> Result<()> {
    if *self.dirty.lock() {
      self.flush_all_chunks()?;
      *self.dirty.lock() = false;
    }

    let meta = self.meta.lock();
    Self::atomic_write_metadata(&self.dir, &*meta)?;
    drop(meta);

    self.maybe_shrink_chunks()?;
    Ok(())
  }

  fn maybe_shrink_chunks(&self) -> Result<()> {
    let meta = self.meta.lock();
    let chunk_size = meta.chunk_size as u64;
    let start_chunk = meta.start / chunk_size;
    let end_chunk = if meta.end == meta.start {
      start_chunk
    } else {
      (meta.end - 1) / chunk_size
    };
    drop(meta);

    let mut chunks = self.chunks.lock();
    let base = *self.base_chunk.lock();
    let mut current_len = chunks.len() as u64;
    if current_len == 0 {
      return Ok(());
    }

    let mut current_start_chunk = base;

    // Remove front chunks if not needed
    while chunks.len() > 1 && current_start_chunk < start_chunk {
      chunks.remove(0);
      current_start_chunk += 1;
    }

    // Remove end chunks if not needed
    while chunks.len() > 1 {
      current_len = chunks.len() as u64;
      let current_end_chunk = current_start_chunk + current_len - 1;
      if current_end_chunk > end_chunk {
        chunks.pop();
      } else {
        break;
      }
    }

    *self.base_chunk.lock() = current_start_chunk;

    Ok(())
  }

  pub fn iter(&self) -> Iter<'_, T> {
    let len = self.len();
    let mut pointers = Vec::with_capacity(len);

    let meta = self.meta.lock();
    let start = meta.start;
    let chunk_size = meta.chunk_size as u64;
    let element_size = meta.element_size;
    drop(meta);

    let base = *self.base_chunk.lock();
    let chunks = self.chunks.lock();
    for i in 0..len {
      let global_idx = start + i as u64;
      let chunk_idx = ((global_idx / chunk_size) - base) as usize;
      let elem_idx = (global_idx % chunk_size) as usize;
      let mmap = &chunks[chunk_idx].mmap;
      let ptr = mmap.as_ptr();
      let elem_ptr = unsafe { ptr.add(elem_idx * element_size) as *const T };
      pointers.push(elem_ptr);
    }
    drop(chunks);

    Iter {
      pointers,
      index: 0,
      len,
      _marker: PhantomData
    }
  }

  pub fn iter_mut(&mut self) -> IterMut<'_, T> {
    let len = self.len();
    let mut pointers = Vec::with_capacity(len);

    let meta = self.meta.lock();
    let start = meta.start;
    let chunk_size = meta.chunk_size as u64;
    let element_size = meta.element_size;
    drop(meta);

    let base = *self.base_chunk.lock();
    let mut chunks = self.chunks.lock();
    for i in 0..len {
      let global_idx = start + i as u64;
      let chunk_idx = ((global_idx / chunk_size) - base) as usize;
      let elem_idx = (global_idx % chunk_size) as usize;
      let mmap = &mut chunks[chunk_idx].mmap;
      let ptr = mmap.as_mut_ptr();
      let elem_ptr = unsafe { ptr.add(elem_idx * element_size) as *mut T };
      pointers.push(elem_ptr);
    }
    drop(chunks);

    IterMut {
      pointers,
      index: 0,
      len,
      _marker: PhantomData
    }
  }
}

pub struct Iter<'a, T: Copy> {
  pointers: Vec<*const T>,
  index: usize,
  len: usize,
  _marker: PhantomData<&'a T>,
}

impl<'a, T: Copy> Iterator for Iter<'a, T> {
  type Item = &'a T;
  fn next(&mut self) -> Option<Self::Item> {
    if self.index < self.len {
      let ptr = self.pointers[self.index];
      self.index += 1;
      unsafe { Some(&*ptr) }
    } else {
      None
    }
  }
}

impl<'a, T: Copy> ExactSizeIterator for Iter<'a, T> {}

pub struct IterMut<'a, T: Copy> {
  pointers: Vec<*mut T>,
  index: usize,
  len: usize,
  _marker: PhantomData<&'a mut T>,
}

impl<'a, T: Copy> Iterator for IterMut<'a, T> {
  type Item = &'a mut T;
  fn next(&mut self) -> Option<Self::Item> {
    if self.index < self.len {
      let ptr = self.pointers[self.index];
      self.index += 1;
      unsafe { Some(&mut *ptr) }
    } else {
      None
    }
  }
}

impl<'a, T: Copy> ExactSizeIterator for IterMut<'a, T> {}
