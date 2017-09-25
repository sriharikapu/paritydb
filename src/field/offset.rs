use field::FieldSize;
use key::Key;

pub fn offset(field_size: &FieldSize, key: &Key) -> usize {
	if field_size.fields_per_page == 0 {
		// big fields
		key.prefix as usize * field_size.size
	} else {
		// small fields
		let page = key.prefix as usize / field_size.fields_per_page;
		let page_offset = FieldSize::PAGE_SIZE * page;
		let page_field = key.prefix as usize % field_size.fields_per_page;
		let page_field_offset = page_field * field_size.size;
		page_offset + page_field_offset
	}
}

#[cfg(test)]
mod tests {
	use field::FieldSize;
	use key::Key;
	use super::offset;

	#[test]
	fn test_offset() {
		assert_eq!(0, offset(&FieldSize::new(99), &Key::new(b"\x00", 8)));
		assert_eq!(100, offset(&FieldSize::new(99), &Key::new(b"\x01", 8)));
		assert_eq!(100, offset(&FieldSize::new(99), &Key::new(b"\x01", 8)));
		assert_eq!(3900, offset(&FieldSize::new(99), &Key::new(b"\x27", 8)));
		assert_eq!(4096, offset(&FieldSize::new(99), &Key::new(b"\x28", 8)));
		assert_eq!(4196, offset(&FieldSize::new(99), &Key::new(b"\x29", 8)));
		assert_eq!(0, offset(&FieldSize::new(4096), &Key::new(b"\x00", 8)));
		assert_eq!(4097, offset(&FieldSize::new(4096), &Key::new(b"\x01", 8)));
		assert_eq!(8194, offset(&FieldSize::new(4096), &Key::new(b"\x02", 8)));
	}
}
