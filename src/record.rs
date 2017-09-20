use byteorder::{LittleEndian, ByteOrder};
use field::view::{FieldsView, FieldsViewMut};

/// Optional size of header for variable-len records.
pub const HEADER_SIZE: usize = 4;

/// Value size
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ValueSize {
	/// Variable record size (needs to be read from header).
	Variable,
	/// Constant record size.
	Constant(usize),
}

/// A view onto database record.
#[derive(Debug)]
pub struct Record<'a> {
	key: FieldsView<'a>,
	value: FieldsView<'a>,
	len: usize,
}

impl<'a> Record<'a> {
	pub fn new(data: &'a [u8], field_body_size: usize, value_size: ValueSize, key_size: usize) -> Self {
		let view = FieldsView::new(data, field_body_size);
		let (key, rest) = view.split_at(key_size);

		match value_size {
			ValueSize::Constant(value_size) => {
				let (value, _) = rest.split_at(value_size);

				Record { key, value, len: value_size }
			},
			ValueSize::Variable => {
				let (header, rest) = rest.split_at(HEADER_SIZE);
				let value_len = Self::read_value_len(header) as usize;
				let (value, _) = rest.split_at(value_len);

				Record { key, value, len: value_len }
			}
		}
	}

	#[inline]
	pub(crate) fn extract_key(data: &'a [u8], field_body_size: usize, key_size: usize) -> FieldsView<'a> {
		FieldsView::with_options(data, field_body_size, 0, key_size)
	}

	fn read_value_len(field: FieldsView<'a>) -> u32 {
		let mut data = [0; HEADER_SIZE];
		field.copy_to_slice(&mut data);
		LittleEndian::read_u32(&data)
	}

	pub fn read_key(&self, slice: &mut [u8]) {
		self.key.copy_to_slice(slice);
	}

	pub fn key_is_equal(&self, slice: &[u8]) -> bool {
		self.key == slice
	}

	pub fn read_value(&self, slice: &mut [u8]) {
		self.value.copy_to_slice(slice);
	}

	pub fn value_len(&self) -> usize {
		self.len
	}
}

/// Mutable view onto database record.
pub struct RecordMut<'a> {
	key: FieldsViewMut<'a>,
	value: FieldsViewMut<'a>,
	current_len: usize,
	variable_len: bool,
}

impl<'a> RecordMut<'a> {
	pub fn new(data: &'a mut [u8], field_body_size: usize, value_size: ValueSize, key_size: usize) -> Self {
		let view = FieldsViewMut::new(data, field_body_size);
		let (key, rest) = view.split_at(key_size);

		match value_size {
			ValueSize::Constant(value_size) => {
				let (value, _) = rest.split_at(value_size);

				RecordMut { key, value, current_len: value_size, variable_len: false }
			},
			ValueSize::Variable => {
				let (header, rest) = rest.split_at(HEADER_SIZE);
				let value_len = Record::read_value_len(header.as_const()) as usize;
				let (value, _) = rest.split_at(value_len);

				RecordMut { key, value, current_len: value_len, variable_len: true }
			}
		}
	}

	pub fn read_key(&self, slice: &mut [u8]) {
		self.key.copy_to_slice(slice);
	}

	pub fn key_is_equal(&self, slice: &[u8]) -> bool {
		self.key == slice
	}

	pub fn read_value(&self, slice: &mut [u8]) {
		self.value.copy_to_slice(slice);
	}

	pub fn write_key(&mut self, slice: &[u8]) {
		self.key.copy_from_slice(slice);
	}

	pub fn write_value(&mut self, slice: &[u8]) {
		if !self.variable_len {
			self.value.copy_from_slice(slice);
		} else {
			// TODO [ToDr] How is it supposed to work?
			unimplemented!()
		}
	}

	pub fn value_len(&self) -> usize {
		self.current_len
	}
}

#[cfg(test)]
mod tests {
	use super::{RecordMut, Record, ValueSize};
	use field;

	#[test]
	fn test_extract_key() {
		let body_size = 8;
		let key_size = 3;
		let data = [
			1, 'a' as u8, 'b' as u8, 'c' as u8, 1, 2, 3, 4, 5,
			1, 'd' as u8, 'e' as u8, 'f' as u8, 6, 7, 8, 9, 10,
		];

		assert_eq!(Record::extract_key(&data, body_size, key_size), &['a' as u8, 'b' as u8, 'c' as u8]);
		assert_eq!(Record::extract_key(&data[body_size + field::HEADER_SIZE..], body_size, key_size), &['d' as u8, 'e' as u8, 'f' as u8]);
	}

	#[test]
	fn test_constant_size_record() {
		let body_size = 8;
		let value_size = ValueSize::Constant(5);
		let key_size = 3;
		let data = [
			1, 'a' as u8, 'b' as u8, 'c' as u8, 1, 2, 3, 4, 5,
			1, 'd' as u8, 'e' as u8, 'f' as u8, 6, 7, 8, 9, 10,
		];
		let mut key = [0; 3];
		let mut value = [0; 5];


		let record = Record::new(&data, body_size, value_size, key_size);
		record.read_key(&mut key);
		assert_eq!(key, ['a' as u8, 'b' as u8, 'c' as u8]);
		assert!(record.key_is_equal(&key));
		assert_eq!(record.value_len(), 5);
		record.read_value(&mut value);
		assert_eq!(value, [1, 2, 3, 4, 5]);

		let record = Record::new(&data[body_size + field::HEADER_SIZE..], body_size, value_size, key_size);
		record.read_key(&mut key);
		assert_eq!(key, ['d' as u8, 'e' as u8, 'f' as u8]);
		assert!(record.key_is_equal(&key));
		assert_eq!(record.value_len(), 5);
		record.read_value(&mut value);
		assert_eq!(value, [6, 7, 8, 9, 10]);
	}

	#[test]
	fn test_variable_size_record() {
		let body_size = 10;
		let value_size = ValueSize::Variable;
		let key_size = 2;
		let data = [
			1, 'a' as u8, 'b' as u8, 3, 0, 0, 0, 1, 2, 3, 99,
			1, 'c' as u8, 'd' as u8, 1, 0, 0, 0, 4, 0, 0, 0,
		];
		let mut key = [0; 2];
		let mut value1 = [0; 3];
		let mut value2 = [0; 1];

		let record1 = Record::new(&data, body_size, value_size, key_size);
		record1.read_key(&mut key);
		assert_eq!(key, ['a' as u8, 'b' as u8]);
		assert!(record1.key_is_equal(&key));
		assert_eq!(record1.value_len(), 3);
		record1.read_value(&mut value1);
		assert_eq!(value1, [1, 2, 3]);

		let record2 = Record::new(&data[body_size + field::HEADER_SIZE..], body_size, value_size, key_size);
		record2.read_key(&mut key);
		assert_eq!(key, ['c' as u8, 'd' as u8]);
		assert!(record2.key_is_equal(&key));
		assert_eq!(record2.value_len(), 1);
		record2.read_value(&mut value2);
		assert_eq!(value2, [4]);
	}

	#[test]
	fn test_record_mut_write() {
		let body_size = 15;
		let value_size = ValueSize::Constant(220);
		let key_size = 20;
		let mut data = [0u8; 256];
		let key = [0x22; 20];
		let value = [0x33; 220];

		let mut written_key = [0u8; 20];
		let mut written_value = [0u8; 220];
		let mut record = RecordMut::new(&mut data, body_size, value_size, key_size);
		record.write_key(&key);
		record.write_value(&value);
		record.read_key(&mut written_key);
		record.read_value(&mut written_value);
		assert_eq!(key, written_key);
		assert_eq!(&value as &[u8], &written_value as &[u8]);
	}
}
