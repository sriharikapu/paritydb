extern crate tempdir;
extern crate accountdb;

use tempdir::TempDir;
use accountdb::Database;

#[test]
fn test_database_open() {
	let temp = TempDir::new("test_database_open").unwrap();
	let _db = Database::open(temp.path()).unwrap();
}
