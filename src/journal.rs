use std::collections::{HashMap, VecDeque};
use std::path::{PathBuf, Path};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::fs::{OpenOptions, File, read_dir};
use std::io::Write;
use std::slice;
use memmap::{Mmap, Protection};
use tiny_keccak::sha3_256;
use transaction::{Transaction, OperationsIterator, Operation};
use error::Result;

#[derive(Debug, PartialEq)]
enum JournalOperation<T> {
	Insert(T),
	Delete,
}

/// Unsafe view onto memmap file memory which backs journal.
struct JournalSlice {
	key: *const u8,
	len: usize,
}

impl JournalSlice {
	fn new(key: &[u8]) -> JournalSlice {
		JournalSlice {
			key: key.as_ptr(),
			len: key.len(),
		}
	}

	unsafe fn as_slice<'a>(&self) -> &'a [u8] {
		slice::from_raw_parts(self.key, self.len)
	}
}

impl Hash for JournalSlice {
	fn hash<H: Hasher>(&self, state: &mut H) {
		unsafe {
			self.as_slice().hash(state);
		}
	}
}

impl PartialEq for JournalSlice {
	fn eq(&self, other: &Self) -> bool {
		unsafe {
			self.as_slice().eq(other.as_slice())
		}
	}
}

impl Eq for JournalSlice {}

unsafe fn cache_memory(memory: &[u8]) -> HashMap<JournalSlice, JournalOperation<JournalSlice>> {
	let iterator = OperationsIterator::new(memory);
	iterator.map(|o| match o {
		Operation::Insert(key, value) => (JournalSlice::new(key), JournalOperation::Insert(JournalSlice::new(value))),
		Operation::Delete(key) => (JournalSlice::new(key), JournalOperation::Delete)
	}).collect()
}

pub struct JournalEra {
	file: PathBuf,
	mmap: Mmap,
	cache: HashMap<JournalSlice, JournalOperation<JournalSlice>>,
}

impl JournalEra {
	fn create<P: AsRef<Path>>(file_path: P, transaction: &Transaction) -> Result<JournalEra> {
		let hash = sha3_256(transaction.raw());
		let mut file = OpenOptions::new()
			.write(true)
			.read(true)
			.create(true)
			.open(&file_path)?;

		file.write_all(&hash)?;
		file.write_all(transaction.raw())?;
		file.flush()?;

		let mmap = Mmap::open(&file, Protection::Read)?;
		let cache = unsafe { cache_memory(&mmap.as_slice()[32..]) };

		let era = JournalEra {
			file: file_path.as_ref().to_path_buf(),
			mmap,
			cache,
		};

		Ok(era)
	}

	fn open<P: AsRef<Path>>(file: P) -> Result<JournalEra> {
		let mmap = Mmap::open_path(&file, Protection::Read)?;
		// TODO: validate checksum here
		let cache = unsafe { cache_memory(&mmap.as_slice()[32..]) };

		let era = JournalEra {
			file: file.as_ref().to_path_buf(),
			mmap,
			cache,
		};

		Ok(era)
	}

	fn get<'a>(&'a self, key: &[u8]) -> Option<JournalOperation<&'a [u8]>> {
		let key = JournalSlice::new(key);

		match self.cache.get(&key) {
			None => None,
			Some(&JournalOperation::Insert(ref value)) => Some(JournalOperation::Insert(unsafe { value.as_slice() })),
			Some(&JournalOperation::Delete) => Some(JournalOperation::Delete),
		}
	}

	fn len(&self) -> usize {
		self.cache.len()
	}

	fn is_empty(&self) -> bool {
		self.cache.is_empty()
	}
}

mod dir {
	use std::fs::read_dir;
	use std::path::{Path, PathBuf};
	use error::Result;

	pub fn era_files<P: AsRef<Path>>(dir: P) -> Result<Vec<PathBuf>> {
		if !dir.as_ref().is_dir() {
			// TODO: err
		}

		// TODO: validate eras consecutiveness

		let mut era_files: Vec<_> = read_dir(dir)?
			.collect::<::std::result::Result<Vec<_>, _>>()?
			.into_iter()
			.map(|entry| entry.path())
			.collect();

		era_files.sort();

		Ok(era_files)
	}

	pub fn next_era_index<P: AsRef<Path>>(files: &[P]) -> Result<u64> {
		match files.last() {
			Some(path) => {
				// .era
				let path = path.as_ref().display().to_string();
				Ok(1u64 + path[..path.len() - 4].parse::<u64>()?)
			},
			None => Ok(0),
		}
	}

	pub fn next_era_filename<P: AsRef<Path>>(dir: P, next_index: u64) -> PathBuf {
		let mut dir = dir.as_ref().to_path_buf();
		dir.push(format!("{}.era", next_index));
		dir
	}
}

pub struct Journal {
	dir: PathBuf,
	max_eras: usize,
	eras: VecDeque<JournalEra>,
	next_era_index: u64,
}

impl Journal {
	pub fn new<P: AsRef<Path>>(jdir: P, max_eras: usize) -> Result<Self> {
		let era_files = dir::era_files(&jdir)?;
		let next_era_index = dir::next_era_index(&era_files)?;

		let eras = era_files.into_iter()
			.map(JournalEra::open)
			.collect::<Result<VecDeque<_>>>()?;

		let journal = Journal {
			dir: jdir.as_ref().to_path_buf(),
			max_eras,
			eras,
			next_era_index,
		};

		Ok(journal)
	}

	pub fn push(&mut self, transaction: &Transaction) -> Result<Option<JournalEra>> {
		let new_path = dir::next_era_filename(&self.dir, self.next_era_index);
		self.next_era_index += 1;
		let new_era = JournalEra::create(new_path, &transaction)?;
		self.eras.push_back(new_era);
		if self.eras.len() > self.max_eras {
			Ok(self.eras.pop_front())
		} else {
			Ok(None)
		}
	}

	pub fn pop(&mut self) -> Result<Option<JournalEra>> {
		Ok(self.eras.pop_back())
	}

	pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a [u8]> {
		for era in self.eras.iter().rev() {
			if let Some(operation) = era.get(&key) {
				return match operation {
					JournalOperation::Insert(insert) => Some(insert),
					JournalOperation::Delete => None,
				}
			}
		}

		None
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;

	use self::tempdir::TempDir;
	use transaction::Transaction;
	use super::{Journal, JournalEra, JournalOperation};

	#[test]
	fn test_era_create() {
		let temp = TempDir::new("test_era_create").unwrap();
		let mut path = temp.path().to_path_buf();
		path.push("file");

		let mut tx = Transaction::default();
		tx.insert(b"key", b"value");
		tx.insert(b"key2", b"value");
		tx.insert(b"key3", b"value");
		tx.insert(b"key2", b"value2");
		tx.delete(b"key3");

		let era = JournalEra::create(path, &tx).unwrap();
		assert_eq!(JournalOperation::Insert(b"value" as &[u8]), era.get(b"key").unwrap());
		assert_eq!(JournalOperation::Insert(b"value2" as &[u8]), era.get(b"key2").unwrap());
		assert_eq!(JournalOperation::Delete, era.get(b"key3").unwrap());
		assert_eq!(None, era.get(b"key4"));
	}

	#[test]
	fn test_journal_new() {
		let temp = TempDir::new("test_journal_new").unwrap();

		let mut journal = Journal::new(temp.path(), 2).unwrap();
		assert!(journal.push(&Transaction::default()).unwrap().is_none());
		assert!(journal.push(&Transaction::default()).unwrap().is_none());
		assert!(journal.push(&Transaction::default()).unwrap().unwrap().is_empty());
	}
}
