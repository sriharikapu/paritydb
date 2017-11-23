#[cfg(test)]
#[macro_use]
extern crate quickcheck;

extern crate paritydb;
extern crate tempdir;

use tempdir::TempDir;
use paritydb::{Database, ValuesLen, Options, Transaction};
use quickcheck::TestResult;

quickcheck! {
    fn can_get_inserted_value(key: Vec<u8>, value: Vec<u8>, key_index_bits: u8) -> TestResult {
        // else we get something like:
        // Error(InvalidOptions("key_index_bits", "53 is greater than key length: 24")
        if key_index_bits as usize > key.len() {
            return TestResult::discard();
        }
        // else we get something like:
        // Error(InvalidOptions(\"key_index_bits\", \"0 is too large. Only prefixes up to 32 bits are supported.\")
        if key_index_bits == 0 {
            return TestResult::discard();
        }
        // limit search space to prevent tests from taking forever
        if key.len() > 16 {
            return TestResult::discard();
        }
        if value.len() > 16 {
            return TestResult::discard();
        }

        let temp = TempDir::new("quickcheck_can_get_inserted_value").unwrap();
        let mut db = Database::create(temp.path(), Options {
            journal_eras: 0,
            key_len: key.len(),
            key_index_bits: key_index_bits,
            value_len: ValuesLen::Constant(value.len()),
            ..Default::default()
        }).unwrap();

        let mut tx = Transaction::default();
        tx.insert(key.clone(), value.clone());

        db.commit(&tx).unwrap();
        db.flush_journal(None).unwrap();

        TestResult::from_bool(db.get(key).unwrap().unwrap() == value)
    }
}
