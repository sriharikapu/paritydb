use std::io::Write;
use std::path::{PathBuf, Path};
use std::{cmp, fs};

use error::{ErrorKind, Result};
use find;
use journal::Journal;
use key::Key;
use memmap::{Mmap, Protection};
use options::{Options, InternalOptions};
use record::Record;
use transaction::Transaction;

const DB_FILE: &str = "data.db";

#[derive(Debug)]
pub enum Value<'a> {
	Raw(&'a [u8]),
	Record(Record<'a>),
}

impl<'a> Value<'a> {
	pub fn to_vec(&self) -> Vec<u8> {
		match *self {
			Value::Raw(ref slice) => slice.to_vec(),
			Value::Record(ref record) => {
				let mut v = Vec::new();
				v.resize(record.value_len(), 0);
				record.read_value(&mut v);
				v
			}
		}
	}
}

// TODO [ToDr] Optimize equality
impl<'a, T: AsRef<[u8]>> PartialEq<T> for Value<'a> {
	fn eq(&self, other: &T) -> bool {
		&*self.to_vec() == other.as_ref()
	}
}

// TODO [ToDr] Optimize equality
impl<'a, 'b> PartialEq<Value<'b>> for Value<'b> {
	fn eq(&self, other: &Value<'b>) -> bool {
		self.to_vec() == other.to_vec()
	}
}

#[derive(Debug)]
pub struct Database {
	journal: Journal,
	options: InternalOptions,
	mmap: Mmap,
	path: PathBuf,
}

impl Database {
	/// Creates new database at given location.
	pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let options = InternalOptions::from_external(options)?;
		let db_file_path = path.as_ref().join(DB_FILE);
		let mut file = fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&db_file_path)?;
		file.set_len(options.initial_db_size)?;
		file.flush()?;

		Self::open(path, options.external)
	}

	/// Opens an existing DB at given location.
	pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let options = InternalOptions::from_external(options)?;
		let journal = Journal::open(&path)?;

		let db_file_path = path.as_ref().join(DB_FILE);
		let mmap = Mmap::open_path(&db_file_path, Protection::ReadWrite)?;

		Ok(Database {
			journal,
			options,
			mmap,
			path: path.as_ref().to_owned(),
		})
	}

	/// Commits changes in the transaction.
	pub fn commit(&mut self, tx: &Transaction) -> Result<()> {
		// TODO [ToDr] Validate key size
		self.journal.push(tx)?;
		Ok(())
	}

	/// Flushes up to `max` excessive journal eras to the disk.
	pub fn flush_journal<T: Into<Option<usize>>>(&mut self, max: T) -> Result<()> {
		let len = self.journal.len();
		let max = max.into().unwrap_or(len);

		if len < self.options.external.journal_eras {
			return Ok(())
		}

		let to_flush = cmp::min(len - self.options.external.journal_eras, max);

		for _era in self.journal.drain_front(to_flush) {
			// TODO [ToDr] Apply era to the database
			unimplemented!()
		}

		Ok(())
	}

	pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Value>> {
		let key = key.as_ref();
		if key.len() != self.options.external.key_len {
			return Err(ErrorKind::InvalidKeyLen(self.options.external.key_len, key.len()).into());
		}

		if let Some(res) = self.journal.get(key) {
			return Ok(Some(Value::Raw(res)));
		}

		let field_body_size = self.options.field_body_size;
		let value_size = self.options.value_size;

		let key = Key::new(key, self.options.external.key_index_bits);
		let offset = key.prefix as usize * self.options.record_offset;
		let data = unsafe { &self.mmap.as_slice()[offset .. ] };

		match find::find_record(data, field_body_size, value_size, key.key)? {
			find::RecordResult::Found(record) => Ok(Some(Value::Record(record))),
			find::RecordResult::NotFound => Ok(None),
			find::RecordResult::OutOfRange => unimplemented!(),
		}
	}
}


#[cfg(test)]
mod tests {
	extern crate tempdir;

	use super::{Database, Options};
	use error::ErrorKind;
	use transaction::Transaction;

	#[test]
	fn create_insert_and_query() {
		let temp = tempdir::TempDir::new("create_insert_and_query").unwrap();

		let mut db = Database::create(temp.path(), Options::with(|mut options| {
			options.journal_eras = 0;
			options.key_len = 3;
		})).unwrap();

		let mut tx = Transaction::default();
		tx.insert("abc", "xyz");
		tx.insert("cde", "123");

		db.commit(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap().unwrap(), b"xyz");
		assert_eq!(db.get("cde").unwrap().unwrap(), b"123");

		// Another transaction
		let mut tx = Transaction::default();
		tx.insert("abc", "456");
		tx.delete("cde");

		db.commit(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap().unwrap(), b"456");
		assert_eq!(db.get("cde").unwrap(), None); // from DB

		// Flush journal and fetch everything from DB.
		// db.flush_journal(2).unwrap();

		// assert_eq!(db.get("abc").unwrap().unwrap(), b"456");
		// assert_eq!(db.get("cde").unwrap(), None);
	}

	#[test]
	fn should_validate_key_length() {
		let temp = tempdir::TempDir::new("create_insert_and_query").unwrap();

		let db = Database::create(temp.path(), Options::with(|mut options| {
			options.journal_eras = 0;
			options.key_len = 3;
		})).unwrap();

		assert_eq!(*db.get("a").unwrap_err().kind(), ErrorKind::InvalidKeyLen(3, 1));
	}
}
