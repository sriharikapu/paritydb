pub const HEADER_SIZE: usize = 1;

/// `Header` is a first byte of database field.
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Header {
	/// Indicates that field hasn't been initialized yet.
	Uninitialized = Header::UNINITIALIZED,
	/// Indicates that the field is the beginning of the record.
	Inserted = Header::INSERTED,
	/// Indicates that the field is continuation of other field which is either `Inserted` or `Deleted`.
	Continued = Header::CONTINUED,
	/// Inficates that the field is the beginning of the deleted record.
	Deleted = Header::DELETED,
}

impl Header {
	const UNINITIALIZED: u8 = 0;
	const INSERTED: u8 = 1;
	const CONTINUED: u8 = 2;
	const DELETED: u8 = 3;

	/// Converts `Header` to u8
	pub fn as_u8(&self) -> u8 {
		*self as u8
	}

	/// Converts `Header` from u8
	pub fn from_u8(byte: u8) -> Option<Header> {
		match byte {
			Self::UNINITIALIZED => Some(Header::Uninitialized),
			Self::INSERTED => Some(Header::Inserted),
			Self::CONTINUED => Some(Header::Continued),
			Self::DELETED => Some(Header::Deleted),
			_ => None,
		}
	}
}
