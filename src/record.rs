use std::slice;

/// A view onto multiple consecutive fields
#[derive(Debug)]
pub struct FieldsView<'a> {
	data: &'a [u8],
	field_body_size: usize,
	offset: usize,
	len: usize,
}

impl<'a> FieldsView<'a> {
	/// Creates new `FieldsView` with no offset
	pub fn new(data: &'a [u8], field_body_size: usize) -> Self {
		FieldsView {
			data,
			field_body_size,
			offset: 0,
			len: data.len() * field_body_size / (field_body_size + 1),
		}
	}

	/// Create new `FieldsView` with an offset. Usefull, when reading record body.
	pub fn with_options(data: &'a [u8], field_body_size: usize, offset: usize, len: usize) -> Self {
		FieldsView {
			data,
			field_body_size,
			offset,
			len,
		}
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.len
	}

	pub fn copy_to_slice(&self, slice: &mut [u8]) {
		assert_eq!(self.len, slice.len(), "slice must have the same size");
		let mut ours = self.offset + self.offset / self.field_body_size;
		let mut theirs = 0;

		if (self.offset % self.field_body_size) != 0 {
			let rem = self.field_body_size - (self.offset % self.field_body_size);
			ours += 1;
			slice[theirs..theirs + rem].copy_from_slice(&self.data[ours..ours + rem]);
			theirs += rem;
			ours += rem;
		}

		let fields = (slice.len() - theirs) / self.field_body_size;
		for _ in 0..fields {
			ours += 1;
			slice[theirs..theirs + self.field_body_size].copy_from_slice(&self.data[ours..ours + self.field_body_size]);
			theirs += self.field_body_size;
			ours += self.field_body_size;
		}

		if theirs != self.len {
			let rem = self.len - theirs;
			ours += 1;
			slice[theirs..].copy_from_slice(&self.data[ours..ours + rem]);
		}
	}

	pub fn split_at(self, pos: usize) -> (Self, Self) {
		let left = FieldsView::with_options(self.data, self.field_body_size, self.offset, pos);
		let right = FieldsView::with_options(self.data, self.field_body_size, self.offset + pos, self.len - pos);
		(left, right)
	}
}

/// A mutable view onto multiple consecutive fields
#[derive(Debug)]
pub struct FieldsViewMut<'a> {
	data: &'a mut [u8],
	field_body_size: usize,
	offset: usize,
	len: usize,
}

impl<'a> FieldsViewMut<'a> {
	/// Creates new `FieldsViewMut` with no offset
	pub fn new(data: &'a mut [u8], field_body_size: usize) -> Self {
		FieldsViewMut {
			len: data.len() * field_body_size / (field_body_size + 1),
			data,
			field_body_size,
			offset: 0,
		}
	}

	/// Create new `FieldsView` with an offset. Usefull, when reading record body.
	pub fn with_options(data: &'a mut [u8], field_body_size: usize, offset: usize, len: usize) -> Self {
		FieldsViewMut {
			data,
			field_body_size,
			offset,
			len,
		}
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.len
	}

	#[inline]
	pub fn as_const(&self) -> FieldsView {
		FieldsView {
			data: self.data,
			field_body_size: self.field_body_size,
			offset: self.offset,
			len: self.len,
		}
	}

	#[inline]
	pub fn copy_to_slice(&self, slice: &mut [u8]) {
		self.as_const().copy_to_slice(slice);
	}

	pub fn copy_from_slice(&mut self, slice: &[u8]) {
		assert_eq!(self.len, slice.len(), "slice must have the same size");
		let mut ours = self.offset + self.offset / self.field_body_size;
		let mut theirs = 0;

		if (self.offset % self.field_body_size) != 0 {
			let rem = self.field_body_size - (self.offset % self.field_body_size);
			ours += 1;
			self.data[ours..ours + rem].copy_from_slice(&slice[theirs..theirs + rem]);
			theirs += rem;
			ours += rem;
		}

		let fields = (slice.len() - theirs) / self.field_body_size;
		for _ in 0..fields {
			ours += 1;
			self.data[ours..ours + self.field_body_size].copy_from_slice(&slice[theirs..theirs + self.field_body_size]);
			theirs += self.field_body_size;
			ours += self.field_body_size;
		}

		if theirs != self.len {
			let rem = self.len - theirs;
			ours += 1;
			self.data[ours..ours + rem].copy_from_slice(&slice[theirs..]);
		}
	}

	pub fn split_at(self, pos: usize) -> (Self, Self) {
		// TODO: left and right part of FieldsViewMut should never access the counterpart, but it would
		// be safer to guarantee that without using unsafe code. It can be done entirely with `slice::split_at_mut`.
		let copied_data = unsafe { slice::from_raw_parts_mut(self.data.as_mut_ptr(), self.data.len()) };
		let left = FieldsViewMut::with_options(copied_data, self.field_body_size, self.offset, pos);
		let right = FieldsViewMut::with_options(self.data, self.field_body_size, self.offset + pos, self.len - pos);
		(left, right)
	}
}

/// A view onto database record.
pub struct Record<'a> {
	key: FieldsView<'a>,
	value: FieldsView<'a>,
}

impl<'a> Record<'a> {
	pub fn new(data: &'a [u8], field_body_size: usize, key_size: usize) -> Self {
		let view = FieldsView::new(data, field_body_size);
		let (key, value) = view.split_at(key_size);
		Record {
			key,
			value,
		}
	}

	pub fn read_key(&self, slice: &mut [u8]) {
		self.key.copy_to_slice(slice);
	}

	pub fn read_value(&self, slice: &mut [u8]) {
		self.value.copy_to_slice(slice);
	}
}

/// Mutable view onto database record.
pub struct RecordMut<'a> {
	key: FieldsViewMut<'a>,
	value: FieldsViewMut<'a>,
}

impl<'a> RecordMut<'a> {
	pub fn new(data: &'a mut [u8], field_body_size: usize, key_size: usize) -> Self {
		let view = FieldsViewMut::new(data, field_body_size);
		let (key, value) = view.split_at(key_size);
		RecordMut {
			key,
			value,
		}
	}

	pub fn read_key(&self, slice: &mut [u8]) {
		self.key.copy_to_slice(slice);
	}

	pub fn read_value(&self, slice: &mut [u8]) {
		self.value.copy_to_slice(slice);
	}

	pub fn write_key(&mut self, slice: &[u8]) {
		self.key.copy_from_slice(slice);
	}

	pub fn write_value(&mut self, slice: &[u8]) {
		self.value.copy_from_slice(slice);
	}
}

#[cfg(test)]
mod tests {
	use std::io::Read;
	use super::{FieldsView, RecordMut};

	#[test]
	fn test_fields_view_copy_to() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let expected = [1, 2, 3, 4, 5, 6];

		let mut result = [0u8; 6];
		let fv = FieldsView::new(&data, body_size);
		fv.copy_to_slice(&mut result);
		assert_eq!(expected, result);
	}

	#[test]
	fn test_fields_view_split_at() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let expected_key = [1, 2];
		let expected_value = [3, 4, 5, 6];

		let mut result_key = [0u8; 2];
		let mut result_value = [0u8; 4];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(2);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_fields_view_split_at2() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let expected_key = [1, 2, 3];
		let expected_value = [4, 5, 6];

		let mut result_key = [0u8; 3];
		let mut result_value = [0u8; 3];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(3);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_fields_view_split_at3() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8, 9, 0, 10, 11];
		let expected_key = [1, 2, 3, 4];
		let expected_value = [5, 6, 7, 8, 9, 10, 11];

		let mut result_key = [0u8; 4];
		let mut result_value = [0u8; 7];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(4);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_fields_view_split_at4() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8, 9, 0, 10, 11, 12, 0, 13];
		let expected_key = [1, 2, 3, 4, 5, 6];
		let expected_value = [7, 8, 9, 10, 11, 12, 13];

		let mut result_key = [0u8; 6];
		let mut result_value = [0u8; 7];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(6);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_record_mut_write() {
		let body_size = 15;
		let key_size = 20;
		let mut data = [0u8; 256];
		let key = [0x22; 20];
		let value = [0x33; 220];
	
		let mut written_key = [0u8; 20];
		let mut written_value = [0u8; 220];
		let mut record = RecordMut::new(&mut data, body_size, key_size);
		record.write_key(&key);
		record.write_value(&value);
		record.read_key(&mut written_key);
		record.read_value(&mut written_value);
		assert_eq!(key, written_key);
		assert_eq!(&value as &[u8], &written_value as &[u8]);
	}
}
