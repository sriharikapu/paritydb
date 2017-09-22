use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use memmap::{Mmap, Protection};
use tiny_keccak::Keccak;

use error::Result;
use key::Key;
use options::InternalOptions;
use transaction::Operation;

#[derive(Debug)]
struct PositiveOperation<'a> {
	offset: usize,
	data: &'a [u8],
}

#[derive(Debug)]
struct PositiveOperationMut<'a> {
	/// offset for operation start within positive operations vector
	operation_start: usize,
	/// data
	data: &'a mut Vec<u8>,
}

impl<'a> PositiveOperationMut<'a> {
	fn new(data: &'a mut Vec<u8>, offset: usize) -> Self {
		let operation_start = data.len();
		data.write_u32::<LittleEndian>(offset as u32).unwrap();
		data.extend_from_slice(&[0; 4]);
		PositiveOperationMut {
			operation_start,
			data,
		}
	}

	fn write(&mut self, data: &[u8]) {
		self.data.extend_from_slice(data);
	}
}

impl<'a> Drop for PositiveOperationMut<'a> {
	fn drop(&mut self) {
		let len = self.data.len() - (self.operation_start + 8);
		LittleEndian::write_u32(&mut self.data[self.operation_start + 4..self.operation_start + 8], len as u32);
	}
}

#[derive(Debug)]
struct PositiveOperationIterator<'a> {
	data: &'a [u8],
}

impl<'a> Iterator for PositiveOperationIterator<'a> {
	type Item = PositiveOperation<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let offset = LittleEndian::read_u32(&self.data[0..4]) as usize;
		let data_len = LittleEndian::read_u32(&self.data[4..8]) as usize;
		let end = 8 + data_len;
		let result = PositiveOperation {
			offset,
			data: &self.data[8..end],
		};

		self.data = &self.data[end..];
		Some(result)
	}
}

enum LocalizedOperationKind<'a> {
	Insert(&'a [u8]),
	Delete,
}

struct LocalizedOperationIterator<T> {
	inner: T,
	prefix_bits: u8,
}

impl<'a, T: Iterator<Item = Operation<'a>>> Iterator for LocalizedOperationIterator<T> {
	type Item = (Key<'a>, LocalizedOperationKind<'a>);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|op| match op {
			Operation::Insert(key, value) => {
				let key = Key::new(key, self.prefix_bits);
				let o = LocalizedOperationKind::Insert(value);
				(key, o)
			},
			Operation::Delete(key) => {
				let key = Key::new(key, self.prefix_bits);
				let o = LocalizedOperationKind::Delete;
				(key, o)
			},
		})
	}
}

/// Stores database operations as a set of only positive operations
#[derive(Debug)]
pub struct Flush {
	path: PathBuf,
	mmap: Mmap,
}

impl Flush {
	const FILE_NAME: &'static str = "db.flush";
	const CHECKSUM_SIZE: usize = 32;

	/// Creates memmap which is a set of only positive operations.
	pub(crate) fn new<'a, I, P>(dir: P, options: &InternalOptions, _db: &[u8], operations: I) -> Result<Flush>
		where I: IntoIterator<Item = Operation<'a>>, P: AsRef<Path> {
		let mut positive_operations = Vec::new();
		let mut iterator = LocalizedOperationIterator {
			inner: operations.into_iter(),
			prefix_bits: options.external.key_index_bits,
		}.peekable();

		// peeked operations to skip
		let mut to_skip = 0;
		while let Some((_key, _kind)) = iterator.next() {
			if to_skip > 0 {
				to_skip -= 1;
				continue;
			}

			let mut _operation = PositiveOperationMut::new(
				&mut positive_operations,
				// TODO: find db insertion offset based on the key
				0,
			);

			// TODO: write a record
			// TODO: create a lazy iterator over next elements which needs to be reorganized
			// on each check peek next operations iterator to check if the next elem should be inserted before
			// if yes, increment to_skip value
		}

		let path = dir.as_ref().to_owned().join(Flush::FILE_NAME);

		let mut file = fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&path)?;
		file.set_len(positive_operations.len() as u64 + Self::CHECKSUM_SIZE as u64)?;
		file.flush()?;

		let mut mmap = Mmap::open(&file, Protection::ReadWrite)?;
		Keccak::sha3_256(&positive_operations, unsafe { &mut mmap.as_mut_slice()[..Self::CHECKSUM_SIZE] });
		unsafe { &mut mmap.as_mut_slice()[Self::CHECKSUM_SIZE..] }.write_all(&positive_operations)?;

		Ok(Flush {
			path,
			mmap,
		})
	}

	pub fn open<P: AsRef<Path>>(dir: P) -> Result<Flush> {
		let path = dir.as_ref().to_owned().join(Self::FILE_NAME);
		let mmap = Mmap::open_path(&path, Protection::Read)?;
		Ok(Flush {
			path,
			mmap,
		})
	}

	pub fn flush(&self, db: &mut [u8]) -> Result<()> {
		let operations = PositiveOperationIterator {
			data: unsafe { &self.mmap.as_slice()[Self::CHECKSUM_SIZE..] },
		};

		for o in operations {
			db[o.offset..o.offset + o.data.len()].copy_from_slice(o.data);
		}

		Ok(())
	}

	pub fn delete(self) -> Result<()> {
		fs::remove_file(self.path)?;
		Ok(())
	}
}
