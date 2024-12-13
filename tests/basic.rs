use mmap_vecdeque::{MmapVecDeque, MmapVecDequeError};
use tempfile::TempDir;

#[test]
fn test_basic_ops() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();
  let mut dq = MmapVecDeque::<u64>::open_or_create(path, None)?;

  // Initially empty
  assert!(dq.is_empty());

  // Push back some elements
  dq.push_back(10)?;
  dq.push_back(20)?;
  dq.push_back(30)?;
  assert_eq!(dq.len(), 3);

  dq.commit()?; // commit changes
  assert_eq!(dq.len(), 3);
  assert_eq!(dq.front(), Some(10));
  assert_eq!(dq.back(), Some(30));

  // Push front
  dq.push_front(5)?;
  dq.commit()?;
  assert_eq!(dq.len(), 4);
  assert_eq!(dq.front(), Some(5));
  assert_eq!(dq.back(), Some(30));

  // Pop front
  let val = dq.pop_front()?;
  dq.commit()?;
  assert_eq!(val, Some(5));
  assert_eq!(dq.front(), Some(10));
  assert_eq!(dq.back(), Some(30));

  // Pop back
  let val = dq.pop_back()?;
  dq.commit()?;
  assert_eq!(val, Some(30));
  assert_eq!(dq.len(), 2);

  dq.clear()?;
  dq.commit()?;
  assert!(dq.is_empty());

  Ok(())
}

#[test]
fn test_iteration() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();
  let mut dq = MmapVecDeque::<u32>::open_or_create(path, None)?;

  // Insert 100 values: 0..99
  for i in 0..100 {
    dq.push_back(i)?;
  }
  dq.commit()?;

  // Verify iter returns correct immutable references
  let collected: Vec<u32> = dq.iter().collect();
  assert_eq!(collected, (0..100).collect::<Vec<u32>>());

  // Use iter_mut to increment each element by 1
  for val in dq.iter_mut() {
    *val += 1;
  }
  dq.commit()?;

  // Now verify that all elements were incremented
  let collected: Vec<u32> = dq.iter().collect();
  assert_eq!(collected, (1..101).collect::<Vec<u32>>());

  Ok(())
}

#[test]
fn test_large_insertions() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();
  let mut dq = MmapVecDeque::<u64>::open_or_create(path, Some(10000))?;

  // Insert 100,000 items
  for i in 0..100_000 {
    dq.push_back(i)?;
  }

  // Not committed yet
  assert_eq!(dq.len(), 100_000);
  dq.commit()?;

  // Verify data
  for (i, val) in dq.iter().enumerate() {
    assert_eq!(val, i as u64);
  }

  // Pop half from the front
  for i in 0..50_000 {
    let front_val = dq.pop_front()?.unwrap();
    assert_eq!(front_val, i as u64);
  }

  dq.commit()?; // commit after popping

  assert_eq!(dq.len(), 50_000);
  assert_eq!(dq.front(), Some(50_000));
  assert_eq!(dq.back(), Some(99_999));

  // Insert more at the front
  for i in (0..50_000).rev() {
    dq.push_front(i + 1_000_000)?;
  }

  dq.commit()?;
  // Now front half are from 1_000_000..1_049_999 and back half are from 50_000..99_999

  assert_eq!(dq.len(), 100_000);
  let front_val = dq.front().unwrap();
  let back_val = dq.back().unwrap();
  assert_eq!(front_val, 1_000_000);
  assert_eq!(back_val, 99_999);

  // Check that front half matches what we expect
  {
    let mut iter = dq.iter();
    // First 50,000 should be 1_000_000..1_049_999
    for i in 0..50_000 {
      let val = iter.next().unwrap();
      assert_eq!(val, 1_000_000 + i as u64);
    }

    // Next 50,000 should be 50_000..99_999
    for i in 50_000..100_000 {
      let val = iter.next().unwrap();
      assert_eq!(val, i as u64);
    }
  }

  dq.clear()?;
  dq.commit()?;
  assert!(dq.is_empty());

  Ok(())
}

#[test]
fn test_push_front_many() -> Result<(), MmapVecDequeError> {
  let tmp = TempDir::new()?;
  let path = tmp.path();
  let mut dq = MmapVecDeque::<i64>::open_or_create(path, None)?;

  // Push a range of negative numbers at the front
  for i in 0..100 {
    dq.push_front(-((i + 1) as i64))?;
  }
  dq.commit()?;

  // Now the front should be -100 and the back should be -1
  assert_eq!(dq.len(), 100);
  assert_eq!(dq.front(), Some(-100));
  assert_eq!(dq.back(), Some(-1));

  let collected: Vec<_> = dq.iter().collect();
  // Should be [-100, -99, ..., -1]
  for (i, &val) in collected.iter().enumerate() {
    assert_eq!(val, -((100 - i) as i64));
  }

  Ok(())
}