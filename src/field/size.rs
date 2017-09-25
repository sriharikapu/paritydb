use field;

#[derive(Debug, PartialEq)]
pub struct FieldSize {
	pub size: usize,
	pub body_size: usize,
	pub page_disproportion: usize,
	pub fields_per_page: usize,
}

impl FieldSize {
	/// page size in bytes
	pub const PAGE_SIZE: usize = 4096;

	pub fn new(body_size: usize) -> Self {
		let size = body_size + field::HEADER_SIZE;
		if size > Self::PAGE_SIZE {
			FieldSize {
				size,
				body_size,
				page_disproportion: 0,
				fields_per_page: 0,
			}
		} else {
			FieldSize {
				size,
				body_size,
				page_disproportion: Self::PAGE_SIZE % size,
				fields_per_page: Self::PAGE_SIZE / size,
			}
		}
	}
}

#[derive(Debug, PartialEq)]
pub struct FieldOffsetIterator {
	size: FieldSize,
	current_offset: usize,
	current_field: usize,
	db_size: usize,
}

impl FieldOffsetIterator {
	pub fn new(size: FieldSize, offset: usize, db_size: usize) -> Self {
		if size.size <= FieldSize::PAGE_SIZE {
			assert!((offset % FieldSize::PAGE_SIZE) % size.size == 0, "FieldOffset initialized with wrong offset; logic error");
		} else {
			assert!(offset % size.size == 0, "FieldOffset initialized with wrong offset; logic error");
		}
		FieldOffsetIterator {
			current_field: (offset % FieldSize::PAGE_SIZE) / size.size,
			size,
			current_offset: offset,
			db_size,
		}
	}
}

impl Iterator for FieldOffsetIterator {
	type Item = usize;

	fn next(&mut self) -> Option<Self::Item> {
		let result = self.current_offset;
		if result + self.size.size > self.db_size {
			return None;
		}

		if self.current_field + 1 < self.size.fields_per_page {
			self.current_field += 1;
			self.current_offset += self.size.size;
		} else {
			self.current_field = 0;
			self.current_offset += self.size.size + self.size.page_disproportion;
		}
		Some(result)
	}
}

#[cfg(test)]
mod tests {
	use super::{FieldSize, FieldOffsetIterator};

	#[test]
	fn test_field_size_init_with_small() {
		let field = FieldSize::new(100);
		assert_eq!(101, field.size);
		assert_eq!(100, field.body_size);
		assert_eq!(56, field.page_disproportion);
		assert_eq!(40, field.fields_per_page);
		assert_eq!(4096, field.page_disproportion + field.fields_per_page * field.size);
	}

	#[test]
	fn test_field_size_init_with_big() {
		let field = FieldSize::new(4096);
		assert_eq!(4097, field.size);
		assert_eq!(4096, field.body_size);
		assert_eq!(0, field.page_disproportion);
		assert_eq!(0, field.fields_per_page);
	}

	#[test]
	fn test_field_offset_iterator_init1() {
		let field = FieldSize::new(99);
		let offset = 100;
		let db_size = 10000;
		let iter = FieldOffsetIterator::new(field, offset, db_size);
		assert_eq!(iter.current_offset, 100);
		assert_eq!(iter.current_field, 1);
	}

	#[test]
	fn test_field_offset_iterator_init2() {
		let field = FieldSize::new(99);
		let offset = 4196;
		let db_size = 10000;
		let iter = FieldOffsetIterator::new(field, offset, db_size);
		assert_eq!(iter.current_offset, 4196);
		assert_eq!(iter.current_field, 1);
	}

	#[test]
	fn test_field_offset_iterator_with_small1() {
		let field = FieldSize::new(99);
		let offset = 3700;
		let db_size = 4296;
		let mut iter = FieldOffsetIterator::new(field, offset, db_size);
		assert_eq!(Some(3700), iter.next());
		assert_eq!(Some(3800), iter.next());
		assert_eq!(Some(3900), iter.next());
		assert_eq!(Some(4096), iter.next());
		assert_eq!(Some(4196), iter.next());
		assert_eq!(None, iter.next());
	}

	#[test]
	fn test_field_offset_iterator_with_small2() {
		let field = FieldSize::new(99);
		let offset = 4096;
		let db_size = 4295;
		let mut iter = FieldOffsetIterator::new(field, offset, db_size);
		assert_eq!(Some(4096), iter.next());
		assert_eq!(None, iter.next());
	}

	#[test]
	fn test_field_offset_iterator_with_big() {
		let field = FieldSize::new(4096);
		let offset = 4097;
		let db_size = 15_000;
		let mut iter = FieldOffsetIterator::new(field, offset, db_size);
		assert_eq!(Some(4097), iter.next());
		assert_eq!(Some(8194), iter.next());
		assert_eq!(None, iter.next());
	}
}
