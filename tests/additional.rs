use mmap_vecdeque::{MmapVecDeque, MmapVecDequeError};
use tempfile::TempDir;

/// Test that attempting to reopen a deque with a different type results in an error.
#[test]
fn test_type_mismatch() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();

  let mut dq_u64 = MmapVecDeque::<u64>::open_or_create(path, None)?;
  dq_u64.push_back(42)?;
  dq_u64.commit()?;
  drop(dq_u64);

  // Attempt to open the same storage as a different type
  let result = MmapVecDeque::<u32>::open_or_create(path, None);
  assert!(result.is_err(), "Expected error due to type mismatch");
  Ok(())
}

/// Test persistence: write data, commit, drop, reopen, and ensure data is still there.
#[test]
fn test_persistence_across_open() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();

  {
    let mut dq = MmapVecDeque::<u64>::open_or_create(path, None)?;
    dq.push_back(100)?;
    dq.push_back(200)?;
    dq.push_front(50)?;
    dq.commit()?;
  }

  // Reopen and check data
  let dq = MmapVecDeque::<u64>::open_or_create(path, None)?;
  assert_eq!(dq.len(), 3);
  assert_eq!(dq.front(), Some(50));
  assert_eq!(dq.back(), Some(200));

  Ok(())
}

/// Test that zero-sized types fail on creation.
#[test]
fn test_zero_sized_type_fails() {
  let tmp = TempDir::new().unwrap();
  let path = tmp.path();

  // Zero-sized type, e.g., ()
  let result = MmapVecDeque::<()>::open_or_create(path, None);
  assert!(result.is_err(), "Zero-sized type should not be supported");
}

/// Test mixed operations: pushing/popping from both ends, committing at intervals.
#[test]
fn test_mixed_operations() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();
  let mut dq = MmapVecDeque::<i32>::open_or_create(path, Some(50))?;

  // Push front and back alternately
  for i in 0..50 {
    dq.push_back(i)?;
    dq.push_front(-i)?;
  }
  dq.commit()?;

  // Now we have 100 elements: front half negative, back half non-negative
  assert_eq!(dq.len(), 100);
  assert_eq!(dq.front(), Some(-49));
  assert_eq!(dq.back(), Some(49));

  // Pop some from front and back
  for _ in 0..10 {
    dq.pop_front()?;
    dq.pop_back()?;
  }
  dq.commit()?;

  // After popping 20 total (10 front, 10 back)
  assert_eq!(dq.len(), 80);

  // Verify the pattern after pops:
  let collected: Vec<i32> = dq.iter().collect();
  // Initially had [-49..=0 (front half), 0..=49 (back half)].
  // Removing 10 from front removes -49..=-40
  // Removing 10 from back removes 40..=49
  // Expected front now: -39 at front, 39 at back
  assert_eq!(collected.len(), 80);
  assert_eq!(collected.first(), Some(&-39));
  assert_eq!(collected.last(), Some(&39));

  Ok(())
}

/// Check operations after clearing
#[test]
fn test_clear_then_reuse() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();
  let mut dq = MmapVecDeque::<u8>::open_or_create(path, None)?;

  dq.push_back(10)?;
  dq.push_back(20)?;
  dq.commit()?;
  assert_eq!(dq.len(), 2);

  dq.clear()?;
  dq.commit()?;
  assert_eq!(dq.len(), 0);

  dq.push_front(99)?;
  dq.push_back(100)?;
  dq.commit()?;
  assert_eq!(dq.len(), 2);
  assert_eq!(dq.front(), Some(99));
  assert_eq!(dq.back(), Some(100));

  Ok(())
}
