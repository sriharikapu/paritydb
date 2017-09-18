use std::cmp;
use std::path::Path;

use error::Result;
use journal::Journal;
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
	pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let journal = Journal::open(&path)?;

		Ok(Database {
			journal,
			options,
		})
	}

	pub fn apply(&mut self, tx: &Transaction) -> Result<()> {
		self.journal.push(tx)?;
		Ok(())
	}

	/// Flushes up to `max` excessive journal eras to the disk.
	pub fn flush_journal(&mut self, max: Option<usize>) -> Result<()> {
		let len = self.journal.len();
		let max = max.unwrap_or(len);

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
			options.journal_eras = 10;
		})).unwrap();

		let mut tx = Transaction::default();
		tx.insert("abc", "xyz");
		tx.insert("cde", "123");

		db.apply(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap(), b"xyz");
		assert_eq!(db.get("cde").unwrap(), b"123");

		let mut tx = Transaction::default();
		tx.insert("abc", "456");
		tx.delete("cde");

		db.apply(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap(), b"456");
		// assert_eq!(db.get("cde"), None);
	}
}
