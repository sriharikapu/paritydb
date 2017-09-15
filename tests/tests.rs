extern crate tempdir;

use tempdir::TempDir;

#[test]
fn test_database_open() {
	let temp = TempDir::new("test_database_open").unwrap();
}
