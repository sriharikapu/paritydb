use std::cmp::Ordering;
use std::collections::btree_set;
use std::io::Write;
use std::path::{PathBuf, Path};
use std::{cmp, fs};

use memmap::{Mmap, Protection};

use error::{ErrorKind, Result};
use find;
use flush::Flush;
use journal::Journal;
use key::Key;
use metadata::{self, Metadata};
use options::{Options, InternalOptions};
use record::Record;
use transaction::{Operation, Transaction};

/// A database record value.
#[derive(Debug, PartialEq)]
pub enum Value<'a> {
	/// Raw (cached/journaled) data
	Raw(&'a [u8]),
	/// DB record
	Record(Record<'a>),
}

impl<'a> Value<'a> {
	/// Allocate a `Vec` with the value.
	pub fn to_vec(&self) -> Vec<u8> {
		match *self {
			Value::Raw(ref slice) => slice.to_vec(),
			Value::Record(ref record) => {
				let mut v = Vec::with_capacity(record.value_len());
				v.resize(record.value_len(), 0);
				record.read_value(&mut v);
				v
			}
		}
	}
}

impl<'a, T: AsRef<[u8]>> PartialEq<T> for Value<'a> {
	fn eq(&self, other: &T) -> bool {
		match *self {
			Value::Raw(slice) => slice == other.as_ref(),
			Value::Record(ref record) => record.value_is_equal(other.as_ref()),
		}
	}
}

impl<'a> From<Record<'a>> for Value<'a> {
	fn from(record: Record<'a>) -> Value<'a> {
		match record.value_raw_slice() {
			Some(raw) => Value::Raw(raw),
			None => Value::Record(record),
		}
	}
}

/// A top-level database API.
#[derive(Debug)]
pub struct Database {
	path: PathBuf,
	options: InternalOptions,
	journal: Journal,
	metadata: Metadata,
	metadata_mmap: Mmap,
	mmap: Mmap,
}

impl Database {
	const DB_FILE: &'static str = "data.db";
	const META_FILE: &'static str = "meta.db";

	/// Creates new database at given location.
	pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let options = InternalOptions::from_external(options)?;

		// Create directories if necessary.
		fs::create_dir_all(&path)?;

		// Create DB file.
		{
			let db_file_path = path.as_ref().join(Self::DB_FILE);
			let mut file = fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&db_file_path)?;
			file.set_len(options.initial_db_size)?;
			file.flush()?;
		}

		// Create Metadata file.
		{
			let meta_file_path = path.as_ref().join(Self::META_FILE);
			let mut file = fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&meta_file_path)?;
			file.set_len(metadata::bytes::len(options.external.key_index_bits) as u64)?;
			file.flush()?;
		}

		Self::open(path, options.external)
	}

	/// Opens an existing DB at given location.
	pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let options = InternalOptions::from_external(options)?;
		let journal = Journal::open(&path)?;

		let db_file_path = path.as_ref().join(Self::DB_FILE);
		let mut mmap = Mmap::open_path(db_file_path, Protection::ReadWrite)?;

		let meta_file_path = path.as_ref().join(Self::META_FILE);
		let mut metadata_mmap = Mmap::open_path(meta_file_path, Protection::ReadWrite)?;

		let mut metadata = metadata::bytes::read(unsafe { metadata_mmap.as_slice() }, options.external.key_index_bits);

		if let Some(flush) = Flush::open(path.as_ref(), options.external.key_index_bits)? {
			flush.flush(unsafe { mmap.as_mut_slice() }, unsafe { metadata_mmap.as_mut_slice() }, &mut metadata);
			mmap.flush()?;
			metadata_mmap.flush()?;
			flush.delete()?;
		}

		Ok(Database {
			path: path.as_ref().to_owned(),
			options,
			journal,
			metadata,
			metadata_mmap,
			mmap,
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

		for era in self.journal.drain_front(to_flush) {
			let flush = Flush::new(
				&self.path,
				&self.options,
				unsafe { self.mmap.as_slice() },
				&self.metadata,
				era.iter(),
			)?;
			era.delete()?;
			// TODO: metadata should be a single structure
			// updating self.metadata should happen after all calls
			// which may fail ("?")
			flush.flush(unsafe { self.mmap.as_mut_slice() }, unsafe { self.metadata_mmap.as_mut_slice() }, &mut self.metadata);
			self.mmap.flush()?;
			self.metadata_mmap.flush()?;
			flush.delete()?;
		}

		Ok(())
	}

	/// Lookup a value associated with given `key`.
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
		if !self.metadata.prefixes.has(key.prefix).unwrap_or(false) {
			return Ok(None);
		}

		let offset = key.prefix as usize * self.options.record_offset;
		let data = unsafe { &self.mmap.as_slice()[offset..] };

		match find::find_record(data, field_body_size, value_size, key.key)? {
			find::RecordResult::Found(record) => Ok(Some(Value::from(record))),
			find::RecordResult::NotFound => Ok(None),
			find::RecordResult::OutOfRange => unimplemented!(),
		}
	}

	/// Returns an iterator over the database key-value pairs.
	pub fn iter(&self) -> Result<DatabaseIterator> {
		let data = unsafe { &self.mmap.as_slice() };
		let occupied_offset_iter = self.metadata.prefixes.offset_iter();
		let field_body_size = self.options.field_body_size;
		let key_size = self.options.external.key_len;
		let value_size = self.options.value_size;

		let record_iter = find::iter(data, occupied_offset_iter, field_body_size, key_size, value_size)?;
		let journal_iter = self.journal.iter();
		let pending = IteratorValue::None;

		Ok(DatabaseIterator { record_iter, journal_iter, pending })
	}
}

#[derive(Debug)]
enum IteratorValue<'a> {
	None,
	Journal(Operation<'a>),
	DB(Record<'a>),
}

impl<'a> IteratorValue<'a> {
	fn take(&mut self) -> Self {
		::std::mem::replace(self, IteratorValue::None)
	}
}

pub struct DatabaseIterator<'a> {
	journal_iter: btree_set::IntoIter<Operation<'a>>,
	record_iter: find::RecordIterator<'a>,
	pending: IteratorValue<'a>,
}

impl<'a> Iterator for DatabaseIterator<'a> {
	type Item = Result<(&'a [u8], Value<'a>)>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let (operation, record) = match self.pending.take() {
				IteratorValue::None => {
					let j = self.journal_iter.next().map_or(IteratorValue::None, IteratorValue::Journal);
					let db = match self.record_iter.next() {
						None => IteratorValue::None,
						Some(Ok(r)) => IteratorValue::DB(r),
						Some(Err(err)) => {
							self.pending = j;
							return Some(Err(err.into()));
						},
					};

					(j, db)
				},
				j @ IteratorValue::Journal(_) => {
					let db = match self.record_iter.next() {
						None => IteratorValue::None,
						Some(Ok(r)) => IteratorValue::DB(r),
						Some(Err(err)) => {
							self.pending = j;
							return Some(Err(err.into()));
						},
					};

					(j, db)
				},
				db @ IteratorValue::DB(_) => {
					let j = self.journal_iter.next().map_or(IteratorValue::None, IteratorValue::Journal);

					(j, db)
				},
			};

			#[inline]
			// returns `None` if the operation is a `Delete` and we should skip to the next value
			fn handle_journal_operation<'a>(o: Operation<'a>) -> Option<Result<(&'a [u8], Value<'a>)>> {
				match o {
					Operation::Delete(_) => {
						None
					},
					Operation::Insert(key, value) => {
						Some(Ok((key, Value::Raw(value))))
					},
				}
			}

			match (operation, record) {
				(IteratorValue::Journal(o), IteratorValue::None) => {
					match handle_journal_operation(o) {
						None => continue,
						s => return s,
					};
				},
				(IteratorValue::None, IteratorValue::DB(r)) => {
					return Some(Ok((r.key_raw_slice(), Value::from(r))));
				},
				(IteratorValue::Journal(o), IteratorValue::DB(r)) => {
					let ord = r.key_cmp(o.key()).expect(
						"only returns None when compared keys don't have the same size; \
						 all keys should have the same size; qed");

					match ord {
						Ordering::Equal => {
							match handle_journal_operation(o) {
								None => continue,
								s => return s,
							};
						},
						Ordering::Greater => {
							self.pending = IteratorValue::DB(r);

							match handle_journal_operation(o) {
								None => continue,
								s => return s,
							};
						},
						Ordering::Less => {
							self.pending = IteratorValue::Journal(o);
							return Some(Ok((r.key_raw_slice(), Value::from(r))));
						},
					};
				},
				(IteratorValue::None, IteratorValue::None) => return None,
				(o, r) => unreachable!("operation: {:?}, record: {:?}", o, r)
			};
		}
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;

	use super::{Database, Options};
	use options::ValuesLen;
	use error::ErrorKind;
	use transaction::Transaction;

	#[test]
	fn create_insert_and_query() {
		let temp = tempdir::TempDir::new("create_insert_and_query").unwrap();

		let mut db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			value_len: ValuesLen::Constant(3),
			..Default::default()
		}).unwrap();

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
		db.flush_journal(2).unwrap();

		assert_eq!(db.get("abc").unwrap().unwrap(), b"456");
		assert_eq!(db.get("cde").unwrap(), None);
	}

	#[test]
	fn should_validate_key_length() {
		let temp = tempdir::TempDir::new("create_insert_and_query").unwrap();

		let db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			..Default::default()
		}).unwrap();

		assert_eq!(*db.get("a").unwrap_err().kind(), ErrorKind::InvalidKeyLen(3, 1));
	}

	#[test]
	fn test_same_key_operation_ordering() {
		let temp = tempdir::TempDir::new("test_fail").unwrap();

		let mut db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			..Default::default()
		}).unwrap();

		let mut tx = Transaction::default();
		tx.insert("abc", "123");
		tx.delete("abc");

		db.commit(&tx).unwrap();
		db.flush_journal(1).unwrap();

		assert_eq!(db.get("abc").unwrap(), None);
	}

	#[test]
	fn test_iter() {
		let temp = tempdir::TempDir::new("test_iter").unwrap();

		let mut db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			value_len: ValuesLen::Constant(3),
			..Default::default()
		}).unwrap();

		let mut tx1 = Transaction::default();
		tx1.insert("abc", "123");
		tx1.insert("def", "467");
		tx1.insert("ghi", "zzz");

		db.commit(&tx1).unwrap();
		db.flush_journal(1).unwrap();

		let mut tx2 = Transaction::default();
		tx2.insert("jkl", "999");
		tx2.insert("def", "333");
		tx2.insert("pqr", "aaa");
		tx2.delete("ghi");

		db.commit(&tx2).unwrap();

		let records = db.iter().unwrap().map(|item| {
			let (k, v) = item.unwrap();
			(::std::str::from_utf8(&k).unwrap().to_string(),
			 ::std::str::from_utf8(&v.to_vec()).unwrap().to_string())
		});

		let expected = vec![
			("abc", "123"),
			("def", "333"),
			("jkl", "999"),
			("pqr", "aaa"),
		];

		assert_eq!(
			records.collect::<Vec<_>>(),
			expected.iter().map(|x| (x.0.to_string(), x.1.to_string())).collect::<Vec<_>>()
		);
	}
}
