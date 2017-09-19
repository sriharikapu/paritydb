use std::io::Write;
use std::path::{PathBuf, Path};
use std::{cmp, fs};

use error::{ErrorKind, Result};
use find;
use journal::Journal;
use memmap::{Mmap, Protection};
use transaction::Transaction;
use options::{Options, InternalOptions};

const DB_FILE: &str = "data.db";

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

		if len < self.options.journal_eras {
			return Ok(())
		}

		let to_flush = cmp::min(len - self.options.journal_eras, max);

		for _era in self.journal.drain_front(to_flush) {
			// TODO [ToDr] Apply era to the database
			unimplemented!()
		}

		Ok(())
	}

	pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<&[u8]>> {
		let key = key.as_ref();
		if key.len() != self.options.key_len {
			return Err(ErrorKind::InvalidKeyLen(self.options.key_len, key.len()).into());
		}

		if let Some(res) = self.journal.get(key) {
			return Ok(Some(res));
		}

		let record_headers = self.options.value_len.is_variable();
		let field_body_size = self.options.field_body_size;
		let data = unsafe { self.mmap.as_slice() };

		match find::find_record_location_for_reading(data, record_headers, field_body_size, key)? {
			find::RecordLocationForReading::Offset(offset) => {
				unimplemented!()
			},
			find::RecordLocationForReading::NotFound => Ok(None),
			// TODO [ToDr] ?
			find::RecordLocationForReading::OutOfRange => unimplemented!(),
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

		let mut db = Database::create(temp.path(), Options::with(|mut options| {
			options.journal_eras = 0;
			options.key_len = 3;
		})).unwrap();

		assert_eq!(*db.get("a").unwrap_err().kind(), ErrorKind::InvalidKeyLen(3, 1));
	}
}
