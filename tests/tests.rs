extern crate tempdir;
extern crate paritydb;

use tempdir::TempDir;
use paritydb::{Database, Options, Transaction, ValuesLen};

#[test]
fn test_database_flush() {
	let temp = TempDir::new("test_database_open").unwrap();

	let mut db = Database::create(temp.path(), Options {
		journal_eras: 0,
		key_len: 3,
		value_len: ValuesLen::Constant(3),
		..Default::default()
	}).unwrap();

	let mut tx = Transaction::default();
	tx.insert("abc", "001");
	tx.insert("abe", "002");
	tx.insert("cde", "003");

	db.commit(&tx).unwrap();
	db.flush_journal(1).unwrap();

	assert_eq!(db.get("abc").unwrap().unwrap(), b"001");
	assert_eq!(db.get("abe").unwrap().unwrap(), b"002");
	assert_eq!(db.get("cde").unwrap().unwrap(), b"003");

	let mut tx = Transaction::default();
	tx.insert("abd", "004");
	db.commit(&tx).unwrap();
	db.flush_journal(1).unwrap();

	assert_eq!(db.get("abd").unwrap().unwrap(), b"004");
	assert_eq!(db.get("abc").unwrap().unwrap(), b"001");
	assert_eq!(db.get("abe").unwrap().unwrap(), b"002");
	assert_eq!(db.get("cde").unwrap().unwrap(), b"003");

	let mut tx = Transaction::default();
	tx.insert("abd", "005");
	db.commit(&tx).unwrap();
	db.flush_journal(1).unwrap();

	assert_eq!(db.get("abd").unwrap().unwrap(), b"005");
}
