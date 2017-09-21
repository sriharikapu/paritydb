use std::{cmp, slice};
use field::header::HEADER_SIZE;

macro_rules! on_body_slice {
	($self:expr, $slice:expr, $fn:ident) => {
		let field_body_size = $self.field_body_size;
		let mut ours = $self.offset + HEADER_SIZE * $self.offset / field_body_size;
		let mut theirs = 0;

		if ($self.offset % field_body_size) != 0 {
			let rem = cmp::min($slice.len(), field_body_size - ($self.offset % field_body_size));
			ours += HEADER_SIZE;

			$fn!($slice[theirs..theirs + rem], $self.data[ours..ours + rem]);

			theirs += rem;
			ours += rem;
		}

		let fields = ($slice.len() - theirs) / field_body_size;
		for _ in 0..fields {
			ours += HEADER_SIZE;

			$fn!($slice[theirs..theirs + field_body_size], $self.data[ours..ours + field_body_size]);

			theirs += field_body_size;
			ours += field_body_size;
		}

		if theirs != $self.len {
			let rem = $self.len - theirs;
			ours += HEADER_SIZE;

			$fn!($slice[theirs..], $self.data[ours..ours + rem]);
		}
	}
}

/// A view onto multiple consecutive fields
#[derive(Debug)]
pub struct FieldsView<'a> {
	data: &'a [u8],
	field_body_size: usize,
	offset: usize,
	len: usize,
}

impl<'a, T: AsRef<[u8]>> PartialEq<T> for FieldsView<'a> {
	fn eq(&self, slice: &T) -> bool {
		let slice = slice.as_ref();
		if slice.len() != self.len {
			return false;
		}

		macro_rules! compare {
			($a: expr, $b: expr) => {
				if &$a != &$b {
					return false;
				}
			}
		}

		on_body_slice!(self, slice, compare);

		true
	}
}

impl<'a, 'b> PartialEq<FieldsView<'b>> for FieldsView<'a> {
	fn eq(&self, other: &FieldsView<'b>) -> bool {
		if self.len != other.len {
			return false;
		}

		if self.data == other.data {
			return self.offset == other.offset;
		}

		// TODO [ToDr] Implement equality between different field views.
		unimplemented!()
	}
}

impl<'a> FieldsView<'a> {
	/// Creates new `FieldsView` with no offset
	pub fn new(data: &'a [u8], field_body_size: usize) -> Self {
		FieldsView {
			data,
			field_body_size,
			offset: 0,
			len: data.len() * field_body_size / (field_body_size + HEADER_SIZE),
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

		macro_rules! copy_to_slice {
			($a: expr, $b: expr) => {
				$a.copy_from_slice(&$b);
			}
		}

		on_body_slice!(self, slice, copy_to_slice);
	}

	pub fn split_at(self, pos: usize) -> (Self, Self) {
		assert!(self.len >= pos, "Cannot split beyond length: {} < {} ", self.len, pos);
		assert!(self.data.len() >= self.offset + pos, "Cannot split beyond data length: {} < {}", self.data.len(), self.offset + pos);

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

impl<'a, T: AsRef<[u8]>> PartialEq<T> for FieldsViewMut<'a> {
	fn eq(&self, slice: &T) -> bool {
		self.as_const() == slice.as_ref()
	}
}

impl<'a> FieldsViewMut<'a> {
	/// Creates new `FieldsViewMut` with no offset
	pub fn new(data: &'a mut [u8], field_body_size: usize) -> Self {
		FieldsViewMut {
			len: data.len() * field_body_size / (field_body_size + HEADER_SIZE),
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

		macro_rules! copy_from_slice {
			($a: expr, $b: expr) => {
				$b.copy_from_slice(&$a);
			}
		}

		on_body_slice!(self, slice, copy_from_slice);
	}

	pub fn split_at(self, pos: usize) -> (Self, Self) {
		assert!(self.len >= pos, "Cannot split beyond FieldsView length: {} < {} ", self.len, pos);
		assert!(self.data.len() >= self.offset + pos, "Cannot split beyond data length: {} < {}", self.data.len(), self.offset + pos);
		// TODO: left and right part of FieldsViewMut should never access the counterpart, but it would
		// be safer to guarantee that without using unsafe code. It can be done entirely with `slice::split_at_mut`.
		let copied_data = unsafe { slice::from_raw_parts_mut(self.data.as_mut_ptr(), self.data.len()) };
		let left = FieldsViewMut::with_options(copied_data, self.field_body_size, self.offset, pos);
		let right = FieldsViewMut::with_options(self.data, self.field_body_size, self.offset + pos, self.len - pos);
		(left, right)
	}
}

#[cfg(test)]
mod tests {
	use super::FieldsView;

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
	fn test_fields_view_split_at_short() {
		let body_size = 5;
		let data = [0, 1, 2, 3, 4, 5];
		let expected_key = [1, 2];
		let expected_value = [3];
		let expected_rest = [4, 5];

		let mut result_key = [0u8; 2];
		let mut result_value = [0u8; 1];
		let mut result_rest = [0u8; 2];

		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(2);
		let (value, rest) = value.split_at(1);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		rest.copy_to_slice(&mut result_rest);
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
		assert_eq!(rest, &expected_rest);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
		assert_eq!(expected_rest, result_rest);
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
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
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
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
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
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
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
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}
}
