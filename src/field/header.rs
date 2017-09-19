const HEADER_UNINITIALIZED: u8 = 0;
const HEADER_INSERTED: u8 = 1;
const HEADER_CONTINUED: u8 = 2;
const HEADER_DELETED: u8 = 3;

pub const HEADER_SIZE: usize = 1;

pub enum Header {
	Uninitialized,
	Inserted,
	Continued,
	Deleted,
}

impl Header {
	pub fn from_u8(byte: u8) -> Option<Header> {
		match byte {
			HEADER_UNINITIALIZED => Some(Header::Uninitialized),
			HEADER_INSERTED => Some(Header::Inserted),
			HEADER_CONTINUED => Some(Header::Continued),
			HEADER_DELETED => Some(Header::Deleted),
			_ => None,
		}
	}
}
