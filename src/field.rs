pub const HEADER_SIZE: usize = 1;

pub const HEADER_UNINITIALIZED: u8 = 0;
pub const HEADER_INSERTED: u8 = 1;
pub const HEADER_CONTINUED: u8 = 2;
pub const HEADER_DELETED: u8 = 3;

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

error_chain! {
	types {
		Error, ErrorKind, ResultExt;
	}

	errors {
		InvalidHeader {
			description("invalid header"),
			display("invalid header"),
		}
		InvalidLength {
			description("invalid length"),
			display("invalid length"),
		}
	}
}

pub struct Field<'a> {
	data: &'a [u8],
}

impl<'a> From<&'a [u8]> for Field<'a> {
	fn from(data: &'a [u8]) -> Self {
		Field {
			data,
		}
	}
}

impl<'a> Field<'a> {
	pub fn header(&self) -> Result<Header, Error> {
		if self.data.is_empty() {
			return Err(ErrorKind::InvalidLength.into());
		}

		Ok(Header::from_u8(self.data[0]).ok_or(ErrorKind::InvalidHeader)?)
	}

	#[inline]
	pub fn is_empty(&self) -> Result<bool, Error> {
		match self.header()? {
			Header::Uninitialized | Header::Deleted => Ok(true),
			_ => Ok(false),
		}
	}

	pub fn body(&self) -> Result<&'a [u8], Error> {
		if self.data.is_empty() {
			return Err(ErrorKind::InvalidLength.into());
		}

		Ok(&self.data[HEADER_SIZE..])
	}
}

#[derive(Clone)]
pub struct FieldIterator<'a> {
	data: &'a [u8],
	field_body_size: usize,
}

impl<'a> FieldIterator<'a> {
	pub fn new(data: &'a [u8], field_body_size: usize) -> Result<Self, Error> {
		if (data.len() % (field_body_size + HEADER_SIZE)) != 0 {
			return Err(ErrorKind::InvalidLength.into());
		}

		Ok(FieldIterator {
			data,
			field_body_size,
		})
	}
}

impl<'a> Iterator for FieldIterator<'a> {
	type Item = Field<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let (next_field, new_data) = self.data.split_at(self.field_body_size + HEADER_SIZE);
		self.data = new_data;
		Some(next_field.into())
	}
}
