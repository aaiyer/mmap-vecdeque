# mmap-vecdeque

`mmap-vecdeque` is a file-backed, memory-mapped, durable, and thread-safe double-ended queue (deque) for Rust. Unlike a normal `VecDeque`, operations aren't immediately durable. Instead, you can batch multiple insertions, deletions, etc. and then call `commit()` to atomically persist all changes to disk.

## Key Features

- **Deferred commits:** Changes are kept in memory until `commit()` is called.
- **Atomic and durable commits:** Once `commit()` returns, all changes are atomically and durably persisted to disk.
- **Configurable chunk size:** Items are stored in fixed-size chunks of elements. By default, 10,000 elements per chunk.
- **Mmap-backed:** Data is accessed via memory mapping for potentially high performance.
- **Iterators:** `iter()` and `iter_mut()` to traverse elements.

## Usage

```rust
use mmap_vecdeque::MmapVecDeque;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let dir = Path::new("my_deque_storage");
    let mut deque = MmapVecDeque::<u64>::open_or_create(dir, None)?;

    deque.push_back(42)?;
    deque.push_front(1)?;
    
    // Changes are not yet durable on disk
    deque.commit()?; // Now 1 and 42 are atomically committed.

    println!("Front: {:?}", deque.front()); // Some(&1)
    println!("Back: {:?}", deque.back());   // Some(&42)

    deque.pop_front()?; 
    // Not durable yet.
    deque.commit()?; // commit again

    println!("Front after pop: {:?}", deque.front()); // Some(&42)

    // Iteration
    for val in deque.iter() {
        println!("{}", val);
    }

    Ok(())
}
```
