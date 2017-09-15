use std::collections::{HashMap, VecDeque};
use std::path::{PathBuf, Path};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::fs::{OpenOptions, File};
use std::io::Write;
use std::slice;
use memmap::{Mmap, Protection};
use tiny_keccak::sha3_256;
use transaction::{Transaction, OperationsIterator};
use error::Result;

enum JournalOperation {
	Insert(*const u8, usize),
	Delete,
}

struct JournalKey {
	key: *const u8,
	len: usize,
}

impl Hash for JournalKey {
	fn hash<H: Hasher>(&self, state: &mut H) {
		unsafe {
			let slice = slice::from_raw_parts(self.key, self.len);
			slice.hash(state);
		}
	}
}

impl PartialEq for JournalKey {
	fn eq(&self, other: &Self) -> bool {
		unsafe {
			let slice = slice::from_raw_parts(self.key, self.len); 
			let oslice = slice::from_raw_parts(other.key, other.len);
			slice.eq(oslice)
		}
	}
}

impl Eq for JournalKey {}

struct Era {
	mmap: Mmap,
	cache: HashMap<JournalKey, JournalOperation>,
}

impl Era {
	fn create<P: AsRef<Path>>(file: P, transaction: &Transaction) -> Result<Era> {
		let hash = sha3_256(transaction.raw());
		let mut file = OpenOptions::new()
			.write(true)
			.read(true)
			.create(true)
			.open(file)?;

		file.write_all(&hash)?;
		file.write_all(transaction.raw())?;
		file.flush()?;

		let mmap = Mmap::open(&file, Protection::Read)?;
		let iterator = unsafe { OperationsIterator::new(&mmap.as_slice()) };
		// TODO: build hashmap
		unimplemented!();

	}

	fn open<P: AsRef<Path>>(file: P) -> Result<Era> {
		let mmap = Mmap::open_path(file, Protection::Read)?;
		let iterator = unsafe { OperationsIterator::new(&mmap.as_slice()) };
		// TODO: build hashmap
		
		unimplemented!();
	}
}

pub struct Journal {
	dir: PathBuf,
	max_eras: usize,
	eras: VecDeque<Era>,
}

impl Journal {
	pub fn new(dir: PathBuf, max_eras: usize) -> Self {
		Journal {
			dir,
			max_eras,
			eras: VecDeque::with_capacity(max_eras),
		}
	}

	pub fn commit(&mut self, transaction: &Transaction) -> Result<()> {
		let new_path: PathBuf = unimplemented!();
		let new_era = Era::create(new_path, &transaction)?;
		self.eras.push_back(new_era);
		Ok(())
	}

	pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a [u8]> {
		let key = JournalKey {
			key: key.as_ptr(),
			len: key.len(),
		};

		for era in self.eras.iter().rev() {
			if let Some(operation) = era.cache.get(&key) {
				return match *operation {
					JournalOperation::Insert(value, len) => Some(unsafe { slice::from_raw_parts(value, len) }),
					JournalOperation::Delete => None,
				}
			}
		}

		None
	}
}
