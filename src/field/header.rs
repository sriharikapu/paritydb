pub const HEADER_SIZE: usize = 1;

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Header {
	Uninitialized = Header::UNINITIALIZED,
	Inserted = Header::INSERTED,
	Continued = Header::CONTINUED,
	Deleted = Header::DELETED,
}

impl Header {
	const UNINITIALIZED: u8 = 0;
	const INSERTED: u8 = 1;
	const CONTINUED: u8 = 2;
	const DELETED: u8 = 3;

	pub fn as_u8(&self) -> u8 {
		*self as u8
	}

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
