mod error;
mod header;
pub mod iterator;
pub mod view;

pub use self::error::{Error, ErrorKind};
pub use self::header::{Header, HEADER_SIZE};

#[inline]
pub fn field_size(field_body_size: usize) -> usize {
	field_body_size + HEADER_SIZE
}

#[inline]
pub fn raw_data_len(len: usize, field_body_size: usize) -> usize {
	let field_size = field_size(field_body_size);
	assert_eq!(len % field_size, 0);
	let fields = len / field_size;
	fields * field_body_size
}
