//! Database 

use std::path::Path;
use std::{self, mem};
use parking_lot::RwLock;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use hashtable::{self, Hashtable, HashtableSlice, Query};
use error::Result;
use file::File;
use {Address, Hash, DATABASE_SLICES};

pub struct Account;
pub struct Code;

struct DatabaseSlice<K, V> {
	hashtable: HashtableSlice,
	file: File<K, V>,
}

impl<K, V> DatabaseSlice<K, V> {
	fn new(hashtable: HashtableSlice, file: File<K, V>) -> Self {
		DatabaseSlice {
			hashtable,
			file,
		}
	}
}

struct AccountDatabaseSlice {
	inner: RwLock<DatabaseSlice<Address, Account>>,
}

impl AccountDatabaseSlice {
	fn new(hashtable: HashtableSlice, file: File<Address, Account>) -> Self {
		AccountDatabaseSlice {
			inner: RwLock::new(DatabaseSlice::new(hashtable, file)),
		}
	}

	fn insert(&self, query: Query<Address>, _account: Account) -> Result<()> {
		let mut inner = self.inner.write();
		// entry is safe, cause it is guarded by a lock
		match unsafe { inner.hashtable.entry(query.slice_query()) } {
			hashtable::Entry::Vacant(entry) => {
			},
			hashtable::Entry::Occupied(entry) => {
			},
		}
		unimplemented!();	
	}

	fn get<F, U>(&self, query: Query<Address>, _callback: F) -> Result<Option<U>> where F: Fn(&Account) -> U {
		let inner = self.inner.read();
		// get is safe, cause it is guarded by a lock
		match unsafe { inner.hashtable.get(query.slice_query()) } {
			Some(position) => {
				unimplemented!();	
			},
			None => Ok(None),
		}
	}

	fn flush(&self) {
		unimplemented!();
	}
}

struct CodeDatabaseSlice {
	inner: RwLock<DatabaseSlice<Hash, Code>>,
}

impl CodeDatabaseSlice {
	fn new(hashtable: HashtableSlice, file: File<Hash, Code>) -> Self {
		CodeDatabaseSlice {
			inner: RwLock::new(DatabaseSlice::new(hashtable, file)),
		}
	}

	fn flush(&self) {
		unimplemented!();
	}
}

pub struct Database {
	account_db_slices: [AccountDatabaseSlice; DATABASE_SLICES],
	code_db_slices: [CodeDatabaseSlice; DATABASE_SLICES],
}

impl Database {
	pub fn open<P: AsRef<Path>>(dir: P) -> Result<Database> {
		let dir = dir.as_ref();
		if !dir.is_dir() {
			return Err(format!("Cannot open database. {} is not a directory.", dir.display()).into());
		}

		let dir_buf = dir.to_path_buf();
		let mut account_hashtable_path = dir_buf.clone();
		account_hashtable_path.push("account_hashtable.accdb");
		let mut code_hashtable_path = dir_buf.clone();
		code_hashtable_path.push("code_hashtable.accdb");

		let account_hashtable = Hashtable::open(account_hashtable_path)?;
		let mut account_hashtable_slices = account_hashtable.into_slices()?;

		let code_hashtable = Hashtable::open(code_hashtable_path)?;
		let mut code_hashtable_slices = code_hashtable.into_slices()?;
		
		let mut account_db_slices: [AccountDatabaseSlice; DATABASE_SLICES] = unsafe { mem::uninitialized() };
		let mut code_db_slices: [CodeDatabaseSlice; DATABASE_SLICES] = unsafe { mem::uninitialized() };

		for i in 0..DATABASE_SLICES {
			let mut account_db_file_path = dir_buf.clone();
			account_db_file_path.push(&format!("account_db_{}.accdb", i));
			let mut code_db_file_path = dir_buf.clone();
			code_db_file_path.push(&format!("code_db_{}.accdb", i));

			// TODO: it `?` try errors here, compiler will try to drop uninitialized memory and everything will crush
			let account_db_file = File::open(account_db_file_path)?;
			let code_db_file = File::open(code_db_file_path)?;

			let account_ht_slice = mem::replace(&mut account_hashtable_slices[i], unsafe { mem::uninitialized() });
			let code_ht_slice = mem::replace(&mut code_hashtable_slices[i], unsafe { mem::uninitialized() });

			let mut account_db_slice = AccountDatabaseSlice::new(account_ht_slice, account_db_file);
			let mut code_db_slice = CodeDatabaseSlice::new(code_ht_slice, code_db_file);

			mem::swap(&mut account_db_slices[i], &mut account_db_slice);
			mem::swap(&mut code_db_slices[i], &mut code_db_slice);
			mem::forget(account_db_slice);
			mem::forget(code_db_slice);
		}

		mem::forget(account_hashtable_slices);
		mem::forget(code_hashtable_slices);

		let database = Database {
			account_db_slices,
			code_db_slices,
		};

		Ok(database)
	}

	pub fn flush(&self) {
		self.code_db_slices.par_iter().for_each(CodeDatabaseSlice::flush);
		self.account_db_slices.par_iter().for_each(AccountDatabaseSlice::flush);
	}

	pub fn insert_account(&self, address: &Address, account: Account) -> Result<()> {
		let query = Query::new(address).expect("address.len() == 20; qed");
		let slice = query.slice_index();
		self.account_db_slices[slice].insert(query, account)
	}

	pub fn get_account<F, U>(&self, address: &Address, callback: F) -> Result<Option<U>> where F: Fn(&Account) -> U {
		let query = Query::new(address).expect("address.len() == 20; qed");
		let slice = query.slice_index();
		self.account_db_slices[slice].get(query, callback)
	}
}
