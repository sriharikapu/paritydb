use std::cmp;
use std::path::Path;

use error::Result;
use journal::Journal;
// use memmap::{Mmap, Protection};
use transaction::Transaction;

#[derive(Default, Debug, PartialEq)]
pub struct Options {
	pub journal_eras: usize,
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

#[derive(Debug)]
pub struct Database {
	journal: Journal,
	options: Options,
}

impl Database {
	/// Creates new database at given location.
	pub fn create<P: AsRef<Path>>(_path: P, _options: Options) -> Result<Self> {
		unimplemented!()

	}

	/// Opens an existing DB at given location.
	pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let journal = Journal::open(&path)?;
		// let _mmap = Mmap::open_path(&path, Protection::Read)?;

		Ok(Database {
			journal,
			options,
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

	pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u8]> {
		if let Some(res) = self.journal.get(key.as_ref()) {
			return Some(res);
		}

		// TODO [ToDr] Check underlying database
		unimplemented!()
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

		let mut db = Database::open(temp.path(), Options::with(|mut options| {
			options.journal_eras = 0;
		})).unwrap();

		let mut tx = Transaction::default();
		tx.insert("abc", "xyz");
		tx.insert("cde", "123");

		db.commit(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap(), b"xyz");
		assert_eq!(db.get("cde").unwrap(), b"123");

		// Another transaction
		let mut tx = Transaction::default();
		tx.insert("abc", "456");
		tx.delete("cde");

		db.commit(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap(), b"456");
		// assert_eq!(db.get("cde"), None); // from DB

		// Flush journal and fetch everything from DB.
		// db.flush_journal(2).unwrap();

		// assert_eq!(db.get("abc").unwrap(), b"456");
		// assert_eq!(db.get("cde"), None);
	}
}
