use std::cmp::Ordering;
use byteorder::{LittleEndian, ByteOrder, WriteBytesExt};

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

	fn key(&self) -> &'a [u8] {
		match *self {
			Operation::Insert(key, _) | Operation::Delete(key) => key,
		}
	}

	/// Each operation is stored in a format which duplicates size before and
	/// after the transaction. Thanks to that, transactions from journal can be
	/// quickly iterated backwards.
	///
	/// ```text
	///  1 byte   4/8 bytes       4/8 bytes  1 byte
	///   /         /                /        /
	/// | type |  size(s) | data | size(s) | type |
	/// ```
	fn write_to_buf(&self, buf: &mut Vec<u8>) {
		match *self {
			Operation::Insert(key, value) => {
				buf.push(Operation::INSERT);
				buf.write_u32::<LittleEndian>(key.len() as u32).unwrap();
				buf.write_u32::<LittleEndian>(value.len() as u32).unwrap();
				buf.extend_from_slice(key);
				buf.extend_from_slice(value);
				buf.write_u32::<LittleEndian>(key.len() as u32).unwrap();
				buf.write_u32::<LittleEndian>(value.len() as u32).unwrap();
				buf.push(Operation::INSERT);
			},
			Operation::Delete(key) => {
				buf.push(Operation::DELETE);
				buf.write_u32::<LittleEndian>(key.len() as u32).unwrap();
				buf.extend_from_slice(key);
				buf.write_u32::<LittleEndian>(key.len() as u32).unwrap();
				buf.push(Operation::DELETE);
			},
		}
	}
}

/// Database operations.
pub struct Transaction {
	operations: Vec<u8>,
}

impl Default for Transaction {
	fn default() -> Self {
		Transaction {
			operations: Vec::new(),
		}
	}
}

impl Transaction {
	/// Append new insert operation to the list of transactions.
	#[inline]
	pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
		self.push(Operation::Insert(key.as_ref(), value.as_ref()));
	}

	/// Append new delete operation to the list of transactions.
	#[inline]
	pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
		self.push(Operation::Delete(key.as_ref()));
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
				self.data = &self.data[value_end + 9..];
				Some(o)
			},
			Operation::DELETE => {
				let key_len = LittleEndian::read_u32(&self.data[1..5]) as usize;
				let key_end = 5 + key_len;
				let o = Operation::Delete(&self.data[5..key_end]);
				self.data = &self.data[key_end + 5..];
				Some(o)
			},
			_ => panic!("Unsupported operation!"),
		}
	}
}

impl<'a> DoubleEndedIterator for OperationsIterator<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let last = self.data.len() - 1;
		match self.data[last] {
			Operation::INSERT => {
				let key_len = LittleEndian::read_u32(&self.data[last - 8..last - 4]) as usize;
				let value_len = LittleEndian::read_u32(&self.data[last - 4..last]) as usize;
				let key_end = last - 8 - value_len;
				let key_begin = key_end - key_len;
				let value_end = key_end + value_len;
				let o = Operation::Insert(&self.data[key_begin..key_end], &self.data[key_end..value_end]);
				self.data = &self.data[..key_begin - 9];
				Some(o)
			},
			Operation::DELETE => {
				let key_len = LittleEndian::read_u32(&self.data[last - 4..last]) as usize;
				let key_end = last - 4;
				let key_begin = key_end - key_len;
				let o = Operation::Delete(&self.data[key_begin..key_end]);
				self.data = &self.data[..key_begin - 5];
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
		let mut t = Transaction::default();
		t.insert(b"key", b"value");
		t.delete(b"key");

		let mut operations = t.operations();

		assert_eq!(operations.next(), Some(Operation::Insert(b"key", b"value")));
		assert_eq!(operations.next(), Some(Operation::Delete(b"key")));
		assert_eq!(operations.next(), None);

		let mut rev_operations = t.operations().rev();

		assert_eq!(rev_operations.next(), Some(Operation::Delete(b"key")));
		assert_eq!(rev_operations.next(), Some(Operation::Insert(b"key", b"value")));
		assert_eq!(rev_operations.next(), None);
	}
}
