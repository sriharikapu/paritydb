use std::io::Write;
use std::path::{PathBuf, Path};
use std::{cmp, fs};

use error::Result;
use find;
use journal::Journal;
use memmap::{Mmap, Protection};
use transaction::Transaction;

#[derive(Debug, PartialEq)]
pub struct Options {
	/// Number of eras to keep in the journal.
	pub journal_eras: usize,
	/// Initial size of the database - the file will be allocated right after the DB is created.
	pub initial_db_size: u64,
	/// The DB will re-allocate to twice as big size in case there is more
	/// than `extend_threshold_percent` occupied entries.
	pub extend_threshold_percent: u8,
	pub field_body_size: usize,
}

impl Default for Options {
	fn default() -> Self {
		Options {
			journal_eras: 5,
			initial_db_size: 512 * 1024 * 1024,
			extend_threshold_percent: 80,
			field_body_size: 63,
		}
	}
}

impl Options {
	pub fn with<F>(f: F) -> Self where
		F: FnOnce(&mut Self),
	{
		let mut options = Options::default();
		f(&mut options);
		options
	}
}

const DB_FILE: &str = "data.db";

#[derive(Debug)]
pub struct Database {
	journal: Journal,
	options: Options,
	mmap: Mmap,
	path: PathBuf,
}

impl Database {
	/// Creates new database at given location.
	pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let db_file_path = path.as_ref().join(DB_FILE);
		let mut file = fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&db_file_path)?;
		file.set_len(options.initial_db_size)?;
		file.flush()?;

		Self::open(path, options)
	}

	/// Opens an existing DB at given location.
	pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
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
		if let Some(res) = self.journal.get(key.as_ref()) {
			return Ok(Some(res));
		}

		let data = unsafe { self.mmap.as_slice() };
		match find::find_record_location_for_reading(data, self.options.field_body_size, key.as_ref())? {
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
	use transaction::Transaction;

	#[test]
	fn create_insert_and_query() {
		let temp = tempdir::TempDir::new("create_insert_and_query").unwrap();

		let mut db = Database::create(temp.path(), Options::with(|mut options| {
			options.journal_eras = 0;
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
}
