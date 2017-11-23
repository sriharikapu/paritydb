use std::cmp::Ordering;
use byteorder::{LittleEndian, ByteOrder, WriteBytesExt};
use error::{ErrorKind, Result};

/// Database operations
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Operation<'a> {
	Insert(&'a [u8], &'a [u8]),
	Delete(&'a [u8]),
}

impl<'a> PartialOrd for Operation<'a> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.key().partial_cmp(other.key())
	}
}

impl<'a> Ord for Operation<'a> {
	fn cmp(&self, other: &Self) -> Ordering {
		self.key().cmp(other.key())
	}
}

impl<'a> Operation<'a> {
	const INSERT: u8 = 0;
	const DELETE: u8 = 1;

	pub fn key(&self) -> &'a [u8] {
		match *self {
			Operation::Insert(key, _) | Operation::Delete(key) => key,
		}
	}

	/// Each operation is stored with a type and size before the transaction.
	///
	/// ```text
	///  1 byte   4/8 bytes
	///   /         /
	/// | type |  size(s) | data |
	/// ```
	fn write_to_buf(&self, buf: &mut Vec<u8>) {
		match *self {
			Operation::Insert(key, value) => {
				buf.push(Operation::INSERT);
				buf.write_u32::<LittleEndian>(key.len() as u32).unwrap();
				buf.write_u32::<LittleEndian>(value.len() as u32).unwrap();
				buf.extend_from_slice(key);
				buf.extend_from_slice(value);
			},
			Operation::Delete(key) => {
				buf.push(Operation::DELETE);
				buf.write_u32::<LittleEndian>(key.len() as u32).unwrap();
				buf.extend_from_slice(key);
			},
		}
	}
}

/// Database operations.
pub struct Transaction {
	/// key length, it's used to determine whether an insert
	/// is valid or not at an early stage, we could probably
	/// use `Options` or `InternalOptions` here, but right now
	/// we only care about key size, so it's enough info.
	key_len: usize,
	operations: Vec<u8>,
}

impl Transaction {
	/// This should only be called in `Database` and some unit tests.
	/// Use `db.create_transaction()` in any other cases.
	pub(crate) fn new(key_len: usize) -> Transaction {
		Transaction {
			key_len: key_len,
			operations: Vec::new(),
		}
	}

	/// Append new insert operation to the list of transactions.
	#[inline]
	pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<()> {
		let key = key.as_ref();
		if key.len() != self.key_len {
			Err(ErrorKind::InvalidKeyLen(self.key_len, key.len()).into())
		} else {
			self.push(Operation::Insert(key, value.as_ref()));
			Ok(())
		}
	}

	/// Append new delete operation to the list of transactions.
	#[inline]
	pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
		let key = key.as_ref();
		if key.len() != self.key_len {
			Err(ErrorKind::InvalidKeyLen(self.key_len, key.len()).into())
		} else {
			self.push(Operation::Delete(key));
			Ok(())
		}
	}

	/// Returns double-ended iterator over all operations in a transaction.
	pub fn operations(&self) -> OperationsIterator {
		OperationsIterator {
			data: &self.operations,
		}
	}

	pub(crate) fn raw(&self) -> &[u8] {
		&self.operations
	}

	#[inline]
	fn push<'a>(&mut self, operation: Operation<'a>) {
		operation.write_to_buf(&mut self.operations);
	}
}

/// Iterator over serialized transaction operations.
/// Operations integrity is guaranteed.
pub struct OperationsIterator<'a> {
	data: &'a [u8],
}

impl<'a> OperationsIterator<'a> {
	/// Unsafety is that data may not contain valid operations
	pub unsafe fn new(data: &'a [u8]) -> Self {
		OperationsIterator {
			data,
		}
	}
}

impl<'a> Iterator for OperationsIterator<'a> {
	type Item = Operation<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		match self.data[0] {
			Operation::INSERT => {
				let key_len = LittleEndian::read_u32(&self.data[1..5]) as usize;
				let value_len = LittleEndian::read_u32(&self.data[5..9]) as usize;
				let key_end = 9 + key_len;
				let value_end = key_end + value_len;
				let o = Operation::Insert(&self.data[9..key_end], &self.data[key_end..value_end]);
				self.data = &self.data[value_end..];
				Some(o)
			},
			Operation::DELETE => {
				let key_len = LittleEndian::read_u32(&self.data[1..5]) as usize;
				let key_end = 5 + key_len;
				let o = Operation::Delete(&self.data[5..key_end]);
				self.data = &self.data[key_end..];
				Some(o)
			},
			_ => panic!("Unsupported operation!"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{Transaction, Operation};

	#[test]
	fn test_transaction() {
		let mut t = Transaction::new(3);
		t.insert(b"key", b"value").unwrap();
		t.delete(b"key").unwrap();

		let mut operations = t.operations();

		assert_eq!(operations.next(), Some(Operation::Insert(b"key", b"value")));
		assert_eq!(operations.next(), Some(Operation::Delete(b"key")));
		assert_eq!(operations.next(), None);
	}

	#[test]
	fn test_transaction_invalid_key_len_for_insert() {
		let mut t = Transaction::new(4);
		assert!(t.insert(b"key", b"value").is_err());
	}

	#[test]
	fn test_transaction_invalid_key_len_for_delete() {
		let mut t = Transaction::new(4);
		assert!(t.delete(b"key").is_err());
	}
}
