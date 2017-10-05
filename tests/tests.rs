extern crate tempdir;
extern crate paritydb;

use tempdir::TempDir;
use paritydb::{Database, Options, Transaction, ValuesLen};

#[derive(Debug)]
enum Action {
	Insert(&'static str, &'static str),
	Delete(&'static str),
	Commit,
	Flush(usize),
	AssertEqual(&'static str, &'static str),
	AssertNone(&'static str),
}

use Action::*;

fn run_actions(test_name: &'static str, actions: &[Action]) {
	let temp = TempDir::new(test_name).unwrap();

	let mut db = Database::create(temp.path(), Options {
		journal_eras: 0,
		key_len: 3,
		value_len: ValuesLen::Constant(3),
		..Default::default()
	}).unwrap();

	let mut tx = Transaction::default();

	for action in actions {
		println!("action: {:?}", action);
		match *action {
			Insert(key, value) => {
				tx.insert(key, value)
			},
			Delete(key) => {
				tx.delete(key)
			},
			Commit => {
				db.commit(&tx).unwrap();
				tx = Transaction::default();
			},
			Flush(eras) => {
				db.flush_journal(eras).unwrap();
			},
			AssertEqual(key, expected_value) => {
				assert_eq!(db.get(key).unwrap().unwrap(), expected_value);
			},
			AssertNone(key) => {
				assert_eq!(db.get(key).unwrap(), None);
			},
		}
	}
}

macro_rules! db_test {
	($name: tt, $($actions: expr),*) => {
		#[test]
		fn $name() {
			run_actions(stringify!($name), &[$($actions),*]);
		}
	}
}

db_test!(
	test_database_flush,
	Insert("abc", "001"),
	Insert("abe", "002"),
	Insert("cde", "003"),
	Commit,
	Flush(1),
	AssertEqual("abc", "001"),
	AssertEqual("abe", "002"),
	AssertEqual("cde", "003"),
	Insert("abd", "004"),
	Commit,
	Flush(1),
	AssertEqual("abc", "001"),
	AssertEqual("abe", "002"),
	AssertEqual("abd", "004"),
	AssertEqual("cde", "003"),
	Insert("abd", "005"),
	Delete("cde"),
	Delete("abc"),
	Commit,
	Flush(1),
	AssertNone("abc"),
	// TODO: shift space
	AssertEqual("abe", "002"),
	AssertEqual("abd", "005"),
	AssertNone("cde")
);

db_test!(
	test_database_flush_shift_only_required1,
	Insert("aaa", "001"),
	Insert("bbb", "002"),
	Commit,
	Flush(1),
	AssertEqual("aaa", "001"),
	AssertEqual("bbb", "002"),
	Delete("aaa"),
	Commit,
	Flush(1),
	AssertNone("aaa"),
	AssertEqual("bbb", "002")
);

db_test!(
	test_database_flush_shift_only_required2,
	Insert("aaa", "001"),
	Insert("bbb", "002"),
	Commit,
	Flush(1),
	AssertEqual("aaa", "001"),
	AssertEqual("bbb", "002"),
	Delete("aaa"),
	Insert("ccc", "003"),
	Commit,
	Flush(1),
	AssertNone("aaa"),
	AssertEqual("bbb", "002"),
	AssertEqual("ccc", "003")
);
