pub struct Field<'a> {
	data: &'a [u8],
}

pub enum Header {
	Uninitialized,
	Insert,
	Continuation,
	Deleted,
}

#[derive(Debug)]
pub enum Error {
	InvalidHeader,
	InvalidLength,
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
			return Err(Error::InvalidLength);
		}

		match self.data[0] {
			0 => Ok(Header::Uninitialized),
			1 => Ok(Header::Insert),
			2 => Ok(Header::Continuation),
			3 => Ok(Header::Deleted),
			_ => Err(Error::InvalidHeader),
		}
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
			return Err(Error::InvalidLength);
		}

		Ok(&self.data[1..])
	}
}

#[derive(Clone)]
pub struct FieldIterator<'a> {
	data: &'a [u8],
	field_body_size: usize,
}

impl<'a> FieldIterator<'a> {
	pub fn new(data: &'a [u8], field_body_size: usize) -> Result<Self, Error> {
		if (data.len() % (field_body_size + 1)) != 0 {
			return Err(Error::InvalidLength);
		}

		let iterator = FieldIterator {
			data,
			field_body_size,
		};

		Ok(iterator)
	}
}

impl<'a> Iterator for FieldIterator<'a> {
	type Item = Field<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let (next_field, new_data) = self.data.split_at(self.field_body_size + 1);
		self.data = new_data;
		Some(next_field.into())
	}
}
